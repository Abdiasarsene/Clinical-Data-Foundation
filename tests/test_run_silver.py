# tests/test_run_silver.py
import pytest
from unittest.mock import patch, MagicMock, ANY
from pipeline.run_silver import run, PipelineFailed

@patch("pipeline.run_silver.list_bronze_tables")
@patch("pipeline.run_silver.PolarsEngine")
@patch("pipeline.run_silver.WriterFactory")
@patch("pipeline.run_silver.compute_row_metrics")
@patch("pipeline.run_silver.report")
def test_run_silver_success(mock_report, mock_metrics, mock_writer_factory,
                            mock_engine_cls, mock_list_tables):
    # Arrange
    mock_list_tables.return_value = ["patients"]
    mock_engine = MagicMock()
    mock_engine_cls.return_value = mock_engine

    # fake dataframe
    fake_df = MagicMock()
    mock_engine.read.return_value = fake_df

    # metrics in/out
    mock_metrics.side_effect = [
        {"row_count": 10},  # metrics_in
        {"row_count": 8},   # metrics_out
    ]

    # fake writer
    mock_writer = MagicMock()
    mock_writer_factory.get_writer.return_value = mock_writer

    # Act
    run()

    # Assert
    mock_engine.read.assert_called_once_with("bronze.patients")
    mock_writer.write.assert_called_once_with(fake_df, mode="OVERWRITE")

    # Vérifier que le reporter a bien loggé le succès
    mock_report.assert_any_call(
        "SUCCESS",
        dataset="patients",
        stage="silver",
        duration_sec=ANY,
        rows_in=10,
        rows_out=8,
    )

@patch("pipeline.run_silver.list_bronze_tables", return_value=["patients"])
@patch("pipeline.run_silver.PolarsEngine")
@patch("pipeline.run_silver.report")
def test_run_silver_failure(mock_report, mock_engine_cls, mock_list_tables):
    # Arrange
    mock_engine = MagicMock()
    mock_engine_cls.return_value = mock_engine
    mock_engine.read.side_effect = Exception("Read error")

    # Act & Assert
    with pytest.raises(PipelineFailed):
        run()

    # Vérifier que le reporter a bien loggé l’échec
    mock_report.assert_any_call(
        "FAILURE",
        dataset="patients",
        stage="silver",
        duration_sec=ANY,
        error="Read error",
    )