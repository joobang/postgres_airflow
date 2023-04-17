from datetime import timedelta
from typing import Any, Dict


DAG_DEFAULT_ARGS: Dict[str, Any] = {
    'weight_rule': 'absolute',
    'owner': 'interviewee',
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'retries': 1,
}
