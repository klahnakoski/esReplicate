SET PYTHONPATH=.
CALL pypy bzETL\replicate.py --settings=resources/settings/replicate_orange_factor.json
CALL pypy bzETL\replicate.py --settings=resources/settings/replicate_comments_settings.json
