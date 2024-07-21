"""
ETL to call api.club_elo.com/team_id api
"""
from datetime import datetime
import csv
import pathlib
import os

import requests


def extract(team_id) -> requests.Response:
    """
    Send a GET request to the club_elo api

    Args:
        team_id (str): Team name in club_elo
    Returns
        Get request response
    """
    return requests.get(url=f"http://api.clubelo.com/{team_id}", timeout=100)


def transform(response: requests.Response) -> str:
    """
    Transform the response into a string

    Args:
        response (requests.Response): response to the API request
    Returns:
        Response string
    Exception:
        When status_code in not valid
    """
    if response.status_code == 200:
        return response.text
    raise Exception(f"status_code {response.status_code}")


def load(text: str, directory: str) -> None:
    """
    Load text to ingest directory as a csv

    Args:
        text (str): Text to be written
        directory (str): Target directory
    Returns:
        None
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    filename = f"{directory}/{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"
    with open(filename, "w", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        for line in text.split("\n"):
            writer.writerow(line.split(","))


def etl(team_id: str, directory: str) -> None:
    """
    Run club_elo ingest ETL

    Args:
        team_id (str): Team name in club_elo
        directory (str): Path to ingested files
    Returns
        None
    """
    load(transform(extract(team_id)), directory)


def main():
    """
    Main entry point

    Returns
        None
    """
    root = pathlib.Path(__file__).parent.parent.parent
    folder = f"{root}/data/raw/club_elo_api"
    etl("Lille", folder)


if __name__ == "__main__":
    main()
