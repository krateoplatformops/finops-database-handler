
import logging
from helpers import format_tags_for_db

def test_format_tags_for_db():
    logger = logging.getLogger(__name__)
    assert format_tags_for_db('\'CostCenter\': \'1234\'; \'org\': \'trey\'', logger) == '{"\'CostCenter\'":"\'1234\'","\'org\'":"\'trey\'"}'
    assert format_tags_for_db('', logger) == '{}' 

