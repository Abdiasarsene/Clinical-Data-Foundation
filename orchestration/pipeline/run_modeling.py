# src/orchestration/pipelines/run_modeling.py
import time
from src.lake.engine.polars_engine import PolarsEngine
from src.modeling.entities.patient import Patient
from src.modeling.entities.encounter import Encounter
from src.modeling.entities.observation import Observation
from src.modeling.relationship.patient_encounters import (
    PatientEncounterRelationship,
)
from src.modeling.relationship.encounter_observations import (
    EncounterObservationRelationship,
)
from src.modeling.temporal.validity import validate_temporal_order
from storage.writer_factory import WriterFactory
from storage.modes import WriteMode
from src.quality.validators import validate_table
from observability.monitoring.events import PipelineEvent
from observability.monitoring.reporter import report
from observability.monitoring.exceptions import PipelineFailed
from observability.logs.logger import get_logger

# ===== LOGGING =====
logger = get_logger("pipeline.modeling")

# ===== SETTINGS =====
DATASET = "healthcare"
STAGE = "modeling"

SILVER_PATH = "lake/silver/"

PATIENTS = "patients"
ENCOUNTERS = "encounters"
OBSERVATIONS = "observations"


# ===== RUN PIPELINE =====
def run():

    start_time = time.time()

    engine = PolarsEngine()

    try:

        report(PipelineEvent.START, dataset=DATASET, stage=STAGE)

        logger.info("Modeling pipeline started", extra={"dataset": DATASET})

        # ===== READ SILVER =====
        patients_df = engine.read(SILVER_PATH + PATIENTS + "/")
        encounters_df = engine.read(SILVER_PATH + ENCOUNTERS + "/")
        observations_df = engine.read(SILVER_PATH + OBSERVATIONS + "/")

        # ===== ENTITY VALIDATION =====
        logger.info("Validating entities")

        for row in patients_df.iter_rows(named=True):
            Patient(**row).validate()

        for row in encounters_df.iter_rows(named=True):
            Encounter(**row).validate()

        for row in observations_df.iter_rows(named=True):
            Observation(**row).validate()

        # ===== RELATIONSHIP VALIDATION =====
        logger.info("Validating relationships")

        patient_ids = set(patients_df["patient_id"])
        encounter_patient_ids = set(encounters_df["patient_id"])

        PatientEncounterRelationship.validate(
            patient_ids,
            encounter_patient_ids,
        )

        encounter_ids = set(encounters_df["encounter_id"])
        observation_encounter_ids = set(observations_df["encounter_id"])

        EncounterObservationRelationship.validate(
            encounter_ids,
            observation_encounter_ids,
        )

        # ===== TEMPORAL VALIDATION =====
        logger.info("Validating temporal consistency")

        for row in encounters_df.iter_rows(named=True):
            validate_temporal_order(
                row["encounter_start"],
                row["encounter_end"],
            )

        # ===== DATA QUALITY =====
        validate_table(
            datasource_name="lake",
            asset_name=PATIENTS,
            suite_name="patients_suite",
        )

        validate_table(
            datasource_name="lake",
            asset_name=ENCOUNTERS,
            suite_name="encounters_suite",
        )

        validate_table(
            datasource_name="lake",
            asset_name=OBSERVATIONS,
            suite_name="observations_suite",
        )

        # ===== WRITE MODELED TABLES =====

        patient_writer = WriterFactory.get_writer(
            layer="modeling",
            engine=engine,
            target=PATIENTS,
        )

        patient_writer.write(
            patients_df,
            mode=WriteMode.MERGE,
            primary_keys=["patient_id"],
        )

        encounter_writer = WriterFactory.get_writer(
            layer="modeling",
            engine=engine,
            target=ENCOUNTERS,
        )

        encounter_writer.write(
            encounters_df,
            mode=WriteMode.MERGE,
            primary_keys=["encounter_id"],
        )

        observation_writer = WriterFactory.get_writer(
            layer="modeling",
            engine=engine,
            target=OBSERVATIONS,
        )

        observation_writer.write(
            observations_df,
            mode=WriteMode.MERGE,
            primary_keys=["observation_id"],
        )

        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.SUCCESS,
            dataset=DATASET,
            stage=STAGE,
            duration_sec=duration,
        )

        logger.info(
            "Modeling pipeline completed",
            extra={"duration_sec": duration},
        )

    except Exception as e:

        duration = round(time.time() - start_time, 2)

        report(
            PipelineEvent.FAILURE,
            dataset=DATASET,
            stage=STAGE,
            duration_sec=duration,
            error=str(e),
        )

        logger.exception("Modeling pipeline failed")

        raise PipelineFailed(str(e)) from e


if __name__ == "__main__":
    run()