# src/modeling/entites/encounter.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Encounter:
    """
    Represents a healthcare interaction.
    Structurally linked to Patient.
    """

    encounter_id: str
    patient_id: str
    encounter_start: datetime
    encounter_end: Optional[datetime]
    encounter_type: Optional[str]
    created_at: datetime

    def validate(self):
        if not self.encounter_id:
            raise ValueError("encounter_id cannot be empty")

        if not self.patient_id:
            raise ValueError("Encounter must reference a patient_id")

        if self.encounter_end and self.encounter_end < self.encounter_start:
            raise ValueError("encounter_end cannot be before encounter_start")