# src/orchestration/pipelines/run_modeling.py
import time
from src.lake.engine.polars_engine import PolarsEngine
from src.modeling.entities.patient import Patient
from src.modeling.entities.encounter import Encounter
from src.modeling.entities.observation import Observation
from src.modeling.relationship.patient_encounters import PatientEncounterRelationship
from src.modeling.relationship.encounter_observations import EncounterObservationRelationship
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
STAGE = "modeling"
SILVER_SCHEMA = "silver"
FEATURE_SCHEMA = "feature"

# dictionnaire table_name → entity_class
TABLES = {
    "patients": Patient,
    "encounters": Encounter,
    "observations": Observation,
}

# ===== RUN PIPELINE =====
def run():
    start_time = time.time()
    engine = PolarsEngine()

    try:
        logger.info("Modeling pipeline started", extra={"stage": STAGE})

        dataframes = {}

        # ===== READ SILVER =====
        for table_name, entity_class in TABLES.items():
            df = engine.read(f"{SILVER_SCHEMA}.{table_name}")  # lecture depuis silver
            dataframes[table_name] = df
            report(PipelineEvent.START, dataset=table_name, stage=STAGE)

        # ===== ENTITY VALIDATION =====
        for table_name, entity_class in TABLES.items():
            df = dataframes[table_name]
            logger.info(f"Validating entity {table_name}")
            for row in df.iter_rows(named=True):
                entity_class(**row).validate()

        # ===== RELATIONSHIP VALIDATION =====
        logger.info("Validating relationships")
        patient_ids = set(dataframes["patients"]["patient_id"])
        encounter_patient_ids = set(dataframes["encounters"]["patient_id"])
        PatientEncounterRelationship.validate(patient_ids, encounter_patient_ids)

        encounter_ids = set(dataframes["encounters"]["encounter_id"])
        observation_encounter_ids = set(dataframes["observations"]["encounter_id"])
        EncounterObservationRelationship.validate(encounter_ids, observation_encounter_ids)

        # ===== TEMPORAL VALIDATION =====
        logger.info("Validating temporal consistency")
        for row in dataframes["encounters"].iter_rows(named=True):
            validate_temporal_order(row["encounter_start"], row["encounter_end"])

        # ===== DATA QUALITY =====
        for table_name in TABLES.keys():
            validate_table(
                datasource_name="lake",
                asset_name=table_name,
                suite_name=f"{table_name}_suite",
            )

        # ===== WRITE MODELED TABLES TO FEATURE =====
        for table_name in TABLES.keys():
            df = dataframes[table_name]
            writer = WriterFactory.get_writer(
                layer="feature",
                engine=engine,
                target=table_name,
                schema=FEATURE_SCHEMA,
            )
            writer.write(
                df,
                mode=WriteMode.MERGE,
                primary_keys=[f"{table_name[:-1]}_id"],  # patient_id, encounter_id, observation_id
            )

            report(
                PipelineEvent.SUCCESS,
                dataset=table_name,
                stage=STAGE,
                duration_sec=round(time.time() - start_time, 2),
            )

        logger.info("Modeling pipeline completed", extra={"stage": STAGE})

    except Exception as e:
        duration = round(time.time() - start_time, 2)
        for table_name in TABLES.keys():
            report(
                PipelineEvent.FAILURE,
                dataset=table_name,
                stage=STAGE,
                duration_sec=duration,
                error=str(e),
            )
        logger.exception("Modeling pipeline failed")
        raise PipelineFailed(str(e)) from e


if __name__ == "__main__":
    run()