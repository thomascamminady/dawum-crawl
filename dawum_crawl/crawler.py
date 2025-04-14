from dataclasses import dataclass
from datetime import datetime

import polars as pl
import requests
from bs4 import BeautifulSoup


@dataclass
class Crawler:
    def __init__(self):
        self._url = "https://www.wahlrecht.de/umfragen/"
        self._endings = [
            "emnid",
            "allensbach",
            "forsa",
            "politbarometer",
            "gms",
            "dimap",
            "insa",
            "yougov",
        ]

        self._df = self._get_df([_[1] for _ in self._get_all_tables()])

    def df(self) -> pl.DataFrame:
        """Return the data."""
        return self._df

    def df_pivot(self) -> pl.DataFrame:
        """Return the data in a pivoted format."""
        return self._pivot_df(self._df)

    def save(self):
        """Save the data to csv files."""
        self._df.write_csv("data/dawum.csv")
        self._pivot_df(self._df).write_csv("data/dawum_pivot.csv")

    def _get_all_tables(self) -> list[tuple[pl.DataFrame, pl.DataFrame]]:
        return [
            self._get_table(self._url + ending + ".htm")
            for ending in self._endings
        ]

    def _get_table(self, url: str) -> tuple[pl.DataFrame, pl.DataFrame]:
        response = requests.get(url, timeout=10)
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table body
        table = soup.select_one("body > table > tbody")

        # Extract the headers (assuming they are in <thead>)
        headers = [
            th.text.strip()
            for th in soup.select("body > table > thead > tr > th")
        ]

        # Extract rows in the table body
        rows = []
        if table is None:
            raise ValueError("No table found")

        for row in table.select("tr"):
            cells = [cell.text.strip() for cell in row.select("td")]
            rows.append(cells)

        # Create DataFrame
        rows_bundestagswahl = [_ for _ in rows if "Bundestagswahl" in _[-1]]
        rows_non_bundestagswahl = [
            _ for _ in rows if "Bundestagswahl" not in _[-1]
        ]

        if "politbarometer" in url:
            headers = [
                "",
                "",
                "CDU/CSU",
                "SPD",
                "GRÜNE",
                "FDP",
                "LINKE",
                "AfD",
                "FW",
                "BSW",
                "Sonstige",
                "",  # this is missing somehow in the original table
                "Befragte",
                "Zeitraum",
            ]

        return (
            pl.DataFrame(
                rows_bundestagswahl, schema=headers[:-1], orient="row"
            ).with_columns(
                URL=pl.lit(url),
                Heruntergeladen=pl.lit(datetime.now().isoformat()),
            ),
            pl.DataFrame(
                rows_non_bundestagswahl, schema=headers, orient="row"
            ).with_columns(
                URL=pl.lit(url),
                Heruntergeladen=pl.lit(datetime.now().isoformat()),
            ),
        )

    def _get_df(self, tables) -> pl.DataFrame:
        return (
            pl.concat(tables, how="diagonal_relaxed")
            .with_columns(
                Datum=(
                    (
                        pl.when(pl.col("URL").str.contains("insa"))
                        .then(pl.col("Datum"))
                        .otherwise(pl.col("column_0"))
                    ).str.to_date("%d.%m.%Y")
                )
            )
            .with_columns(
                (
                    pl.when(pl.col("Nichtwähler/Unentschl.") == "")
                    .then(pl.lit("0%"))
                    .otherwise(pl.col("Nichtwähler/Unentschl."))
                ).alias("Nichtwähler/Unentschl."),
                Sonstige=(
                    pl.col("Sonstige")
                    .str.replace("%", "% ")
                    .str.strip_chars(" ")
                    .str.split(" %")
                    .list.reverse()
                    .list.tail(-1)
                ),
            )
            .with_columns(
                (
                    pl.col(
                        "CDU/CSU",
                        "SPD",
                        "GRÜNE",
                        "FDP",
                        "LINKE",
                        "AfD",
                        "FW",
                        "BSW",
                        "Nichtwähler/Unentschl.",
                    )
                    .str.replace_all("%", "")
                    .str.replace_all(",", ".")
                    .str.replace_all(" ", "")
                    .str.replace("–", "0")
                    .cast(pl.Float32)
                ),
            )
            .with_columns(
                BSW=(
                    pl.col("Sonstige").map_elements(
                        lambda _list: [
                            float(_.replace("BSW ", ""))
                            for _ in _list
                            if "BSW" in _
                        ][0]
                        if any("BSW" in _ for _ in _list)
                        else 0.0,
                        return_dtype=pl.Float32,
                    )
                ),
                FW=(
                    pl.col("Sonstige").map_elements(
                        lambda _list: [
                            float(_.replace("FW ", ""))
                            for _ in _list
                            if "FW" in _
                        ][0]
                        if any("FW" in _ for _ in _list)
                        else 0.0,
                        return_dtype=pl.Float32,
                    )
                ),
                PIR=(
                    pl.col("Sonstige").map_elements(
                        lambda _list: [
                            float(_.replace("PIR ", ""))
                            for _ in _list
                            if "PIR" in _
                        ][0]
                        if any("PIR" in _ for _ in _list)
                        else 0.0,
                        return_dtype=pl.Float32,
                    )
                ),
                Sonstige=(
                    pl.col("Sonstige")
                    .list.first()
                    .str.replace("Son ", "")
                    .str.replace("Sonst. ", "")
                    .str.replace_all(" ", "")
                    .str.replace_all(",", ".")
                    .cast(pl.Float32)
                ),
                Umfang=(
                    pl.when(pl.col("Befragte") == "Bundestagswahl")
                    .then(pl.lit(0))
                    .otherwise(
                        pl.col("Befragte")
                        # .str.split("•").list.last()
                        .str.replace_all("T • ", "")
                        .str.replace_all("O • ", "")
                        .str.replace_all("TOM • ", "")
                        # .str.replace("\.", "")
                        .str.replace_all(r"\.", "")
                        .map_elements(
                            lambda _: 0 if _ == "" else int(_),
                            return_dtype=pl.Int32,
                        )
                    )
                ),
                Methode=(
                    pl.when(pl.col("Befragte") == "Bundestagswahl")
                    .then(pl.lit("Bundestagswahl"))
                    .otherwise(
                        pl.when(pl.col("Befragte").str.contains("T •"))
                        .then(pl.lit("T"))
                        .otherwise(
                            pl.when(pl.col("Befragte").str.contains("O •"))
                            .then(pl.lit("O"))
                            .otherwise(
                                pl.when(
                                    pl.col("Befragte").str.contains("TOM •")
                                )
                                .then(pl.lit("TOM"))
                                .otherwise(pl.lit(""))
                            )
                        )
                    )
                ),
                URL=(
                    pl.col("URL")
                    .str.split("/")
                    .list.last()
                    .str.split(".")
                    .list.first()
                    .str.to_uppercase()
                ),
            )
            .select(
                "Datum",
                "URL",
                "CDU/CSU",
                "SPD",
                "GRÜNE",
                "FDP",
                "LINKE",
                "AfD",
                "FW",
                "BSW",
                "PIR",
                "Sonstige",
                "Befragte",
                "Methode",
                "Umfang",
                # "Heruntergeladen",
            )
            .sort("Datum", "URL", descending=False, nulls_last=True)
        )

    def _pivot_df(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.unpivot(
            index=["Datum", "URL", "Umfang", "Methode"],
            on=[
                "CDU/CSU",
                "SPD",
                "GRÜNE",
                "FDP",
                "LINKE",
                "AfD",
                "FW",
                "BSW",
                "Sonstige",
            ],
            variable_name="Partei",
            value_name="Anteil",
        )
