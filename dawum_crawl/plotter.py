import altair as alt
import polars as pl

alt.data_transformers.disable_max_rows()


class Plotter:
    def __init__(self):
        self.color_mapping = {
            "BSW": "#ee7930",
            "LINKE": "#D33495",
            "SPD": "#D12C24",
            "GRÜNE": "#739E3E",
            "FDP": "#FBE24C",
            "CDU/CSU": "#010101",
            "FW": "orange",
            "AfD": "#4BAACF",
            "Sonstige": "#C0C0C0",
            # "Andere": "#E6E6E6",
            # "Others": "#E6E6E6",
        }

    def plot(self, df: pl.DataFrame) -> alt.LayerChart:
        """Plot the data."""
        selection = alt.selection_point(fields=["Partei"], bind="legend")

        base = (
            alt.Chart(df.fill_null(0))
            # .mark_line(interpolate="monotone")
            # .mark_point(filled=True, opacity=0.1)
            .encode(
                x=alt.X(shorthand="yearmonth(Datum):T"),
                y=alt.Y("mean(Anteil):Q").scale(domain=[0, 50]),
                color=alt.Color("Partei:N")
                .scale(
                    domain=list(self.color_mapping.keys()),
                    range=list(self.color_mapping.values()),
                )
                .legend(symbolOpacity=1),
                # size=alt.Size("Umfang:Q"),
                # row=alt.Ro w("Partei:N"),
                # detail=alt.Detail("URL:N"),
                opacity=alt.condition(
                    selection, alt.value(0.5), alt.value(0.05)
                ),
            )
            .add_params(selection)
            .properties(width=1200, height=400)
        )

        return alt.layer(
            base.mark_errorband(
                extent="stdev", interpolate="monotone", clip=True
            ).encode(),
            base.mark_line(
                interpolate="monotone", clip=True, filled=False
            ).encode(detail="URL:N"),
        )
