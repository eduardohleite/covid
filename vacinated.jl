using CSV, DataFrames, Query, Pipe

df = CSV.File("vaccinated_1.csv") |> DataFrame
@pipe CSV.File("vaccinated_2.csv") |> DataFrame |> append!(df, _)
@pipe CSV.File("vaccinated_3.csv") |> DataFrame |> append!(df, _)

df_1dose = df |>
                @filter(_.vacina_descricao_dose[1][1] == '1') |>
                @groupby({_.vacina_dataaplicacao}) |>
                @map({data=key(_)[1], count=length(_)}) |>
                DataFrame

df_total = df |>
                @filter(_.vacina_descricao_dose[1][1] == '2' || _.vacina_nome == "Vacina covid-19 - Ad26.COV2.S - Janssen-Cilag") |>
                @groupby({_.vacina_dataaplicacao}) |>
                @map({data=key(_)[1], count=length(_)}) |>
                DataFrame


df_vac =  df_1dose |>
            @orderby(_.data) |>
            @join(df_total, _.data, _.data, {data=_.data, parcial=_.count, total=__.count}) |>
            DataFrame

CSV.write("vacinados_br.csv", df_vac)