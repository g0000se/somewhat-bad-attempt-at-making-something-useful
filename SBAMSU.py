import pandas as pd
import numpy as np
import datetime

previousMonth = (datetime.date.today()-datetime.timedelta(25)).strftime("%m")
exportName = previousMonth + " EoP Report" + ".xlsx"
dfServ = pd.read_excel("allServ.xlsx")

#df1 i df2 su originalni reporti od Inceptuma
df1 = pd.read_excel("EX08.xlsx")
df2 = pd.read_excel("EX09.xlsx")

#kako nije moguće raditi "element wise" merge ako nisu jednaki DF-ovi, potrebno ih je napraviti...
#1. spajam puni outer join u novi DF kako bi dobio jednake elemente u oba DF-a i zadržavam samo potrebne stupce (NEMA USLUGA)
df3 = pd.merge(df2, df1, how="outer", on=["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"])
df3 = df3.loc[:, ["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"]]

#2. novi puni outer join samo sa korisnicima i svim mogućim uslugama. Ovo se radi ako primjerice u n mjesecu je samo
#   jedan korisnik imao neku uslugu, te je imao Churn u idućem mjesecu. Da nema ovoga merge-a, skripta ne bi radila.
dfAllServ = pd.merge(df3, dfServ, how="outer", on=["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"])

#3. Radim faktički ispravni df za n-1 mjesec
dfTemp = pd.merge(df3, df1, how="left", on=["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"])
df4 = pd.concat([dfTemp, dfServ], sort=True)

#4. Radim faktički ispravni df za n mjesec
dfTemp1 = pd.merge(df3, df2, how="left", on=["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"])
df5 = pd.concat([dfTemp1, dfServ], sort=True)

def diff_pd(df5, df4):
    assert (df5.columns == df4.columns).all(), \
        "Ako se ovo pojavi, nešto je gadno spetljano"
    if any(df5.dtypes != df4.dtypes):
        df4 = df4.astype(df5.dtypes)
    if df5.equals(df4):
        return None
    else:
        diff_mask = (df5 != df4) & ~(df5.isnull() & df4.isnull())
        ne_stacked = diff_mask.stack()
        changed = ne_stacked[ne_stacked]
        changed.index.names = ['ID', "Service name"]
        difference_locations = np.where(diff_mask)
        changed_from = df5.values[difference_locations]
        changed_to = df4.values[difference_locations]
        return pd.DataFrame({'Previous EoP': changed_from, "Current EoP": changed_to},
                            index=changed.index)


df6 = diff_pd(df4, df5)

#nevažno jel df5 ili df4 radi toga što su potpuno jednaki "element wise"
df5 = df5.loc[:, ["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"]]

df7 = pd.merge(df6, df5, how="left", left_on=["ID"], right_index=True)

df1 = df1.loc[:, ["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"]]
df2 = df2.loc[:, ["Footprint", "OIB", "Naziv korisnika", "LocationId", "Adresa Lokacije"]]

df8 = df2.merge(df1, indicator = True, how='outer').loc[lambda x : x['_merge']!='both']
df8 = df8.loc[:, ["_merge"]]
df8 = df8.rename(columns={"_merge": "Location status"})
df8 = df8['Location status'].replace({'left_only':'GA', 'right_only':'Churn'})

df9 = pd.merge(df7, df8, how="left", left_on=["ID"], right_index=True)
df9 = df9.reset_index()
df9['Location status'] = df9['Location status'].fillna(value="SC")

df9.to_excel(exportName, sheet_name="Raw_data", index=False)