# src/modeling/relationship/patient_encounter.py 
class PatientEncounterRelationship:
    """
    Defines integrity rules between Patient and Encounter.
    """

    @staticmethod
    def validate(patient_ids: set, encounter_patient_ids: set):
        missing = encounter_patient_ids - patient_ids
        if missing:
            raise ValueError(
                f"Encounters reference unknown patients: {missing}"
            )