# tests/test_run_ingestion.py
import pytest
from unittest.mock import patch, MagicMock
from pipeline.run_ingestion import run_ingestion, PipelineFailed

@patch("src.orchestration.pipelines.run_ingestion.create_schemas")
@patch("src.orchestration.pipelines.run_ingestion.store_raw_data_healthcare")
@patch("src.orchestration.pipelines.run_ingestion.ensure_bucket")
@patch("src.orchestration.pipelines.run_ingestion.upload_directory")
@patch("src.orchestration.pipelines.run_ingestion.MinioToPostgresIngestor")
@patch("src.orchestration.pipelines.run_ingestion.report")
def test_run_ingestion_success(mock_report, mock_ingestor_cls, mock_upload, mock_ensure, mock_store, mock_create):
    # Arrange
    mock_store.return_value = MagicMock()  # fake minio client
    mock_ingestor = MagicMock()
    mock_ingestor_cls.return_value = mock_ingestor

    # Act
    run_ingestion()

    # Assert: vérifier que toutes les étapes sont appelées
    mock_create.assert_called_once()
    mock_store.assert_called_once()
    mock_ensure.assert_called_once()
    mock_upload.assert_called_once()
    mock_ingestor.ingest_bucket.assert_called_once_with("healthcare-raw-data")

    # Vérifier que le reporter a bien loggé le succès final
    mock_report.assert_any_call("SUCCESS", pipeline="ingestion")

@patch("src.orchestration.pipelines.run_ingestion.create_schemas", side_effect=Exception("DB error"))
@patch("src.orchestration.pipelines.run_ingestion.report")
def test_run_ingestion_failure(mock_report, mock_create):
    # Act & Assert: l’exception doit être transformée en PipelineFailed
    with pytest.raises(PipelineFailed):
        run_ingestion()

    # Vérifier que le reporter a bien loggé l’échec
    mock_report.assert_any_call("FAILURE", pipeline="ingestion", error="DB error")