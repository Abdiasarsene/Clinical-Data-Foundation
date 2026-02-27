# src/modeling/entities/observation.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Observation:
    """
    Clinical measurement recorded during an encounter.
    """

    observation_id: str
    encounter_id: str
    patient_id: str
    observed_at: datetime
    observation_type: str
    value: float
    unit: Optional[str]

    def validate(self):
        if not self.observation_id:
            raise ValueError("observation_id is required")

        if not self.encounter_id:
            raise ValueError("observation must reference encounter_id")

        if not self.patient_id:
            raise ValueError("observation must reference patient_id")