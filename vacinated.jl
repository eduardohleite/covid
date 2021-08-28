using CSV, DataFrames, Query

df = CSV.File("part-00000-056aba60-c251-4c73-a081-7294a3bf0b08.c000") |> DataFrame

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

#CSV.write("1dose_sc.csv", df_1dose)
#CSV.write("full_sc.csv", df_total)

#df_1dose = CSV.File("1dose_sc.csv") |> DataFrame
#df_total = CSV.File("full_sc.csv") |> DataFrame

df_vac =  df_1dose |>
            @orderby(_.data) |>
            @join(df_total, _.data, _.data, {estado="SC", data=_.data, parcial=_.count, total=__.count}) |>
            DataFrame

CSV.write("vacinados_sc.csv", df_vac)