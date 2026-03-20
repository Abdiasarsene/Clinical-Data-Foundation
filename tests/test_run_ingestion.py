import pytest
from unittest.mock import patch, MagicMock
from pipeline.run_ingestion import run_ingestion, PipelineFailed, RAW_BUCKET
from observability.monitoring.events import PipelineEvent


@patch("pipeline.run_ingestion.create_schemas")
@patch("pipeline.run_ingestion.store_raw_data_healthcare")
@patch("pipeline.run_ingestion.ensure_bucket")
@patch("pipeline.run_ingestion.upload_directory")
@patch("pipeline.run_ingestion.MinioToPostgresIngestor")
@patch("pipeline.run_ingestion.report")
def test_run_ingestion_success(
    mock_report,
    mock_ingestor_cls,
    mock_upload,
    mock_ensure,
    mock_store,
    mock_create
):
    # Arrange
    mock_store.return_value = MagicMock()
    mock_ingestor = MagicMock()
    mock_ingestor_cls.return_value = mock_ingestor

    # Act
    run_ingestion()

    # Assert
    mock_create.assert_called_once()
    mock_store.assert_called_once()
    mock_ensure.assert_called_once()
    mock_upload.assert_called_once()
    mock_ingestor.ingest_bucket.assert_called_once_with(RAW_BUCKET)

    mock_report.assert_any_call(PipelineEvent.SUCCESS, pipeline="ingestion")


@patch("pipeline.run_ingestion.create_schemas", side_effect=Exception("DB error"))
@patch("pipeline.run_ingestion.report")
def test_run_ingestion_failure(mock_report, mock_create):

    with pytest.raises(PipelineFailed):
        run_ingestion()

    mock_report.assert_any_call(
        PipelineEvent.FAILURE,
        pipeline="ingestion",
        error="DB error"
    )