
run:
	python manage.py runserver

worker:
	celery -A celery_app worker --loglevel=info --pool=solo
