from datetime import datetime

from airflow.decorators import dag, task
import pendulum
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

import requests

@dag(
    dag_id='podcast_summary',
    schedule_interval='@daily',
    start_date=pendulum.datetime(2023, 11, 10),
    catchup=False
)
def podcast_summary():
    def obtain_apple_api_token():
        # Replace with your Apple Podcasts API credentials and token acquisition logic
        apple_token_url = "https://api.apple.com/token"
        apple_token_payload = 'grant_type=client_credentials&client_id="2162fe68e1034b90aa1be9b6411188d9"&client_secret="56c67aa6c8e34cffb2b1d57ad7477277"'
        apple_token_headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Bearer ' + requests.post(apple_token_url, headers={}, data=apple_token_payload).json().get('access_token')
        }
        return apple_token_headers

    APPLE_PODCASTS_API_URL = "https://api.apple.com/podcasts/your_podcast_id"
    headers = obtain_apple_api_token()

    create_db_operator = SqliteOperator(
        task_id="create_table_sqlite",
        sqlite_conn_id="podcasts",
        sql=r"""
        CREATE TABLE IF NOT EXISTS podcast_episodes(
            episode_id TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            summary TEXT,
            duration_ms INTEGER,
            is_explicit INTEGER,
            external_link TEXT,
            html_summary TEXT,
            release_date DATE,
            date_precision TEXT,
            content_type TEXT,
            uri TEXT,
            is_external_hosted INTEGER,
            is_playable INTEGER,
            language TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            CHECK (date_precision IN ('year', 'month', 'day'))
        )
        """
    )

    @task()
    def fetch_and_display_episodes():
        response = requests.get(url=APPLE_PODCASTS_API_URL, headers=headers)
        episodes = response.json().get('episodes')
        print(f"Fetched episodes of type {type(episodes)}")
        print(episodes)
        print(f"Found {len(episodes)} episodes.")
        return episodes

    podcast_episodes = fetch_and_display_episodes()
    create_db_operator.set_downstream(podcast_episodes)

    @task()
    def insert_episodes_into_db(episodes):
        db_hook = SqliteHook(sqlite_conn_id="podcasts")
        existing_episodes = db_hook.get_pandas_df("SELECT * FROM podcast_episodes")
        new_episodes_data = []

        for episode in episodes['items']:
            new_episodes_data.append((
                episode.get('title'),
                episode.get('description'),
                episode.get('duration_ms'),
                episode.get('explicit'),
                episode.get('external_urls').get('apple'),
                episode.get('html_description'),
                episode.get('release_date'),
                episode.get('release_date_precision'),
                episode.get('type'),
                episode.get('uri'),
                episode.get('is_externally_hosted'),
                episode.get('is_playable'),
                episode.get('language'),
                datetime.utcnow(),  # created_at
                datetime.utcnow()  # updated_at
            ))

        if new_episodes_data:
            db_hook.insert_rows(table="podcast_episodes", rows=new_episodes_data,
                                target_fields=["title", "summary", "duration_ms", "is_explicit", "external_link",
                                               "html_summary", "release_date", "date_precision", "content_type", "uri",
                                               "is_external_hosted", "is_playable", "language", "created_at",
                                               "updated_at"])

    insert_episodes_into_db(podcast_episodes)


dag = podcast_summary()
