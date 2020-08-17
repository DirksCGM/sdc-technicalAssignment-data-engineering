import jobs
import pipelines


def run_jobs():
    jobs.newsapi_job()


def run_pipelines():
    pipelines.newsapi_etl()


if __name__ == '__main__':
    run_jobs()
    run_pipelines()
