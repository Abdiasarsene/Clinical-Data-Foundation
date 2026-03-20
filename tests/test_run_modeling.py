# tests/test_run_modeling.py
import pytest
from unittest.mock import patch, MagicMock, ANY
from pipeline.run_modeling import run, PipelineFailed

@patch("pipeline.run_modeling.PolarsEngine")
@patch("pipeline.run_modeling.WriterFactory")
@patch("pipeline.run_modeling.report")
@patch("pipeline.run_modeling.PatientEncounterRelationship")
@patch("pipeline.run_modeling.EncounterObservationRelationship")
@patch("pipeline.run_modeling.validate_temporal_order")
@patch("pipeline.run_modeling.validate_table")
def test_run_modeling_success(mock_validate_table, mock_validate_temporal, mock_enc_obs_rel, mock_pat_enc_rel, mock_report, mock_writer_factory,mock_engine_cls):
    # Arrange
    mock_engine = MagicMock()
    mock_engine_cls.return_value = mock_engine

    # fake dataframes
    patients_df = MagicMock()
    patients_df.iter_rows.return_value = [{"patient_id": 1}]
    patients_df.__getitem__.side_effect = lambda key: [1] if key == "patient_id" else []

    encounters_df = MagicMock()
    encounters_df.iter_rows.return_value = [{"encounter_id": 10, "patient_id": 1,"encounter_start": "2020-01-01","encounter_end": "2020-01-02"}]
    encounters_df.__getitem__.side_effect = lambda key: [10] if key == "encounter_id" else [1]

    observations_df = MagicMock()
    observations_df.iter_rows.return_value = [{"observation_id": 100, "encounter_id": 10}]
    observations_df.__getitem__.side_effect = lambda key: [10] if key == "encounter_id" else [100]

    mock_engine.read.side_effect = [patients_df, encounters_df, observations_df]

    # fake writer
    mock_writer = MagicMock()
    mock_writer_factory.get_writer.return_value = mock_writer

    # Act
    run()

    # Assert: lecture des 3 tables
    assert mock_engine.read.call_count == 3
    # écriture des 3 tables
    assert mock_writer.write.call_count == 3
    # validations appelées
    mock_pat_enc_rel.validate.assert_called_once()
    mock_enc_obs_rel.validate.assert_called_once()
    mock_validate_temporal.assert_called()
    mock_validate_table.assert_called()

    # reporter succès
    mock_report.assert_any_call(
        "SUCCESS",
        dataset="patients",
        stage="modeling",
        duration_sec=ANY,
    )

@patch("pipeline.run_modeling.PolarsEngine")
@patch("pipeline.run_modeling.report")
def test_run_modeling_failure(mock_report, mock_engine_cls):
    # Arrange
    mock_engine = MagicMock()
    mock_engine_cls.return_value = mock_engine
    mock_engine.read.side_effect = Exception("Read error")

    # Act & Assert
    with pytest.raises(PipelineFailed):
        run()

    # reporter échec pour chaque table
    mock_report.assert_any_call(
        "FAILURE",
        dataset="patients",
        stage="modeling",
        duration_sec=ANY,
        error="Read error",
    )