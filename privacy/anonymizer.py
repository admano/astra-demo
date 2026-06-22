
from venv import logger

from models import PrivacyRunRequest
from detector import RawEntity
from dataclasses import dataclass
from vault import PseudonymVault


@dataclass
class AnonymizationResult:
    entity_type: str
    original_value: str
    pseudonym: str
    action :str
    token_ref :str


def pseudonymize(
    entities:list[RawEntity], 
    text: str, 
    vault: PseudonymVault, 
    session_id: str,
) -> tuple[str, list[AnonymizationResult]]:
    
    if not entities:
        return text, []
    
    actions : list[AnonymizationResult]=[]
    for entity in entities :
        pseudonym = vault.pseudonymize(session_id, entity.entity_type, entity.value)
        actions.append(AnonymizationResult(
            entity_type = entity.entity_type,
            original_value= entity.value,
            pseudonym= pseudonym,
            action = "pseudonymized",
            token_ref = pseudonym
        ))

    try:
        anonymized_text = _presidio_replace(entities,actions,text)
    except Exception as e:
        logger.warning(f"Presidio replacement failed: {e}")
        anonymized_text = _offset_replace(entities,actions,text)

    return anonymized_text, actions

def _presidio_replace(
    entities: list[RawEntity],
    actions:  list[AnonymizationResult],
    text:     str,
) -> str:
    from presidio_anonymizer import AnonymizerEngine
    from presidio_anonymizer.entities import RecognizerResult, OperatorConfig

    engine          = AnonymizerEngine()
    analyzer_results = []
    operators:  dict[str, OperatorConfig] = {}

    for entity, action in zip(entities, actions):
        analyzer_results.append(RecognizerResult(
            entity_type = entity.entity_type,
            start       = entity.start,
            end         = entity.end,
            score       = entity.confidence,
        ))
        operators[entity.entity_type] = OperatorConfig(
            "replace", {"new_value": action.pseudonym}
        )

    result = engine.anonymize(
        text             = text,
        analyzer_results = analyzer_results,   # ← correct param name
        operators        = operators,
    )
    return result.text

def _offset_replace(entities:list[RawEntity], actions: list[AnonymizationResult], text:str,) -> str:
    pairs = sorted(zip(entities,actions), key=lambda x :x[0].start)
    result = text
    offset =0
    for entity, action in pairs:
        s = entity.start + offset
        e = entity.end + offset
        result = result[:s]+ action.pseudonym + result[:e]
        offset += len(action.pseudonym)- (entity.end - entity.start)

    return result