import pandas as pd
from datetime import datetime
import os


def add_holiday(records, desc, start, end, academies, zone, school_year="2025-2026"):
    """
    Add a holiday record to the records list
    
    Summary:
        Adds a holiday record with the specified details to the provided records list
    
    Args:
        records (list): List to append the holiday record to
        desc (str): Description of the holiday
        start (str): Start date in ISO format
        end (str): End date in ISO format
        academies (str): Academic region name
        zone (str): Country/region code
        school_year (str, optional): School year. Defaults to "2025-2026"
    
    Returns:
        None: Modifies the records list in-place
    
    Remarks:
       Alexander VALENCIA - 19/04/2025
    """

    # Convert start and end dates to ISO format
    records.append({
        "Description": desc,
        "Population": "-",
        "Date de début": start,
        "Date de fin": end,
        "Académies": academies,
        "Zones": zone,
        "annee_scolaire": school_year
    })

# ──────────────────────────────────────────────────────────────────────────────

def create_country_holidays():
    """
    Create holiday records for various countries
    
    Summary:
        Creates a list of holiday records for different countries and regions
        including Belgium, Germany, Switzerland, Italy, Spain and Luxembourg
    
    Args:
        None
    
    Returns:
        list: A list of dictionaries containing holiday information
    
    Remarks:
        Alexander VALENCIA - 19/04/2025
    """

    # Create a list to hold holiday records
    records = []
    
    # Belgium – Dutch (Flanders)
    add_holiday(records, "Herfst", "2025-10-27T00:00:00+01:00", "2025-11-02T00:00:00+01:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Wapenstilstand", "2025-11-11T00:00:00+01:00", "2025-11-12T00:00:00+01:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Kerst", "2025-12-22T00:00:00+01:00", "2026-01-04T00:00:00+01:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Krokus", "2026-02-16T00:00:00+01:00", "2026-02-22T00:00:00+01:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Paas", "2026-04-06T00:00:00+02:00", "2026-04-19T00:00:00+02:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "1 Mei", "2026-05-01T00:00:00+02:00", "2026-05-02T00:00:00+02:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Hemelvaart", "2026-05-14T00:00:00+02:00", "2026-05-15T00:00:00+02:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Pinkstermaandag", "2026-05-25T00:00:00+02:00", "2026-05-26T00:00:00+02:00", "Vlaanderen", "BE_NL")
    add_holiday(records, "Zomer", "2026-07-01T00:00:00+02:00", "2026-08-31T00:00:00+02:00", "Vlaanderen", "BE_NL")

    # Germany – Bavaria
    add_holiday(records, "Sommerferien", "2025-08-01T00:00:00+02:00", "2025-09-15T00:00:00+02:00", "Bayern", "DE_BY")
    add_holiday(records, "Allerheiligen", "2025-11-03T00:00:00+01:00", "2025-11-07T00:00:00+01:00", "Bayern", "DE_BY")
    add_holiday(records, "Weihnachtsferien", "2025-12-22T00:00:00+01:00", "2026-01-05T00:00:00+01:00", "Bayern", "DE_BY")
    add_holiday(records, "Frühjahrsferien", "2026-02-16T00:00:00+01:00", "2026-02-20T00:00:00+01:00", "Bayern", "DE_BY")
    add_holiday(records, "Osterferien", "2026-03-30T00:00:00+02:00", "2026-04-10T00:00:00+02:00", "Bayern", "DE_BY")
    add_holiday(records, "Pfingstferien", "2026-05-26T00:00:00+02:00", "2026-06-05T00:00:00+02:00", "Bayern", "DE_BY")

    # Switzerland – Canton Zurich
    add_holiday(records, "Weihnachten", "2025-12-22T00:00:00+01:00", "2026-01-02T00:00:00+01:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Sportferien", "2026-02-09T00:00:00+01:00", "2026-02-20T00:00:00+01:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Osterferien", "2026-04-02T00:00:00+02:00", "2026-04-06T00:00:00+02:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Frühlingsferien", "2026-04-20T00:00:00+02:00", "2026-05-01T00:00:00+02:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Auffahrtsferien", "2026-05-14T00:00:00+02:00", "2026-05-15T00:00:00+02:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Pfingstmontag", "2026-05-25T00:00:00+02:00", "2026-05-26T00:00:00+02:00", "Kanton Zürich", "CH_ZH")
    add_holiday(records, "Sommerferien", "2026-07-13T00:00:00+02:00", "2026-08-14T00:00:00+02:00", "Kanton Zürich", "CH_ZH")

    # Italy – South Tyrol (Bolzano)
    add_holiday(records, "Ponte Ognissanti", "2025-10-31T00:00:00+01:00", "2025-11-01T00:00:00+01:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Immacolata", "2025-12-08T00:00:00+01:00", "2025-12-09T00:00:00+01:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Natale", "2025-12-22T00:00:00+01:00", "2026-01-06T00:00:00+01:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Carnevale & Olimpiadi", "2026-02-16T00:00:00+01:00", "2026-02-18T00:00:00+01:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Pasqua", "2026-04-02T00:00:00+02:00", "2026-04-08T00:00:00+02:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Liberazione", "2026-04-24T00:00:00+02:00", "2026-04-25T00:00:00+02:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Festa del Lavoro", "2026-05-01T00:00:00+02:00", "2026-05-02T00:00:00+02:00", "Alto Adige", "IT_BZ")
    add_holiday(records, "Festa della Repubblica", "2026-06-02T00:00:00+02:00", "2026-06-03T00:00:00+02:00", "Alto Adige", "IT_BZ")

    # Spain – Galicia
    add_holiday(records, "Navidad", "2025-12-22T00:00:00+01:00", "2026-01-08T00:00:00+01:00", "Galicia", "ES_GA")
    add_holiday(records, "Semana Santa", "2026-04-06T00:00:00+02:00", "2026-04-13T00:00:00+02:00", "Galicia", "ES_GA")
    add_holiday(records, "Verano", "2026-06-22T00:00:00+02:00", "2026-09-09T00:00:00+02:00", "Galicia", "ES_GA")

    # Luxembourg
    add_holiday(records, "Toussaint", "2025-11-01T00:00:00+01:00", "2025-11-09T00:00:00+01:00", "Luxembourg", "LU")
    add_holiday(records, "Saint-Nicolas", "2025-12-06T00:00:00+01:00", "2025-12-07T00:00:00+01:00", "Luxembourg", "LU")
    add_holiday(records, "Noël", "2025-12-20T00:00:00+01:00", "2026-01-04T00:00:00+01:00", "Luxembourg", "LU")
    add_holiday(records, "Carnaval", "2026-02-14T00:00:00+01:00", "2026-02-22T00:00:00+01:00", "Luxembourg", "LU")
    add_holiday(records, "Pâques", "2026-03-28T00:00:00+02:00", "2026-04-12T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "1 Mai", "2026-05-01T00:00:00+02:00", "2026-05-02T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "Europe", "2026-05-09T00:00:00+02:00", "2026-05-10T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "Ascension", "2026-05-14T00:00:00+02:00", "2026-05-14T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "Pentecôte", "2026-05-23T00:00:00+02:00", "2026-05-31T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "Anniv. Grand-Duc", "2026-06-23T00:00:00+02:00", "2026-06-24T00:00:00+02:00", "Luxembourg", "LU")
    add_holiday(records, "Été", "2026-07-16T00:00:00+02:00", "2026-09-14T00:00:00+02:00", "Luxembourg", "LU")
    
    return records

# ──────────────────────────────────────────────────────────────────────────────

def save_to_csv(df, file_path="../fr-en-calendrier-scolaire-remaining.csv"):
    """
    Save a DataFrame to CSV file
    
    Summary:
        Exports the provided DataFrame to a CSV file at the specified path,
        creating any necessary directories if they don't exist
    
    Args:
        df (pd.DataFrame): DataFrame to save
        file_path (str): Path where the CSV will be saved. Defaults to "../fr-en-calendrier-scolaire-remaining.csv"
    
    Returns:
        None: The function prints a confirmation message but does not return a value
    
    Remarks:
        Alexander VALENCIA - 19/04/2025
    """

    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # utf-8-sig encoding to handle special characters
    # and ensure compatibility with Excel
    df.to_csv(file_path, sep=";", index=False, encoding="utf-8-sig")
    print(f"CSV file saved to: {file_path}")


if __name__ == "__main__":
    records = create_country_holidays()
    df = pd.DataFrame.from_records(records)
    save_to_csv(df)
