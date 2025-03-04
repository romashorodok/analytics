from celery import Celery

app = Celery("analytics_app", broker="redis://localhost:6379/0")

app.autodiscover_tasks(["tasks"])

if __name__ == "__main__":
    app.start()
