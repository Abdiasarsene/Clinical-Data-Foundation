# src/modeling/entities/patient.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class Patient:
    """
    Technical entity definition.
    No clinical interpretation.
    """

    patient_id: str
    birth_date: Optional[datetime]
    gender: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime] = None

    def validate(self):
        if not self.patient_id:
            raise ValueError("patient_id cannot be empty")

        if self.birth_date and self.birth_date > datetime.utcnow():
            raise ValueError("birth_date cannot be in the future")