# src/modeling/relationship/encounter_observation.py
class EncounterObservationRelationship:
    """
    Ensures observation belongs to existing encounter.
    """

    @staticmethod
    def validate(encounter_ids: set, observation_encounter_ids: set):
        missing = observation_encounter_ids - encounter_ids
        if missing:
            raise ValueError(
                f"Observations reference unknown encounters: {missing}"
            )