# 📚 BH naučne publikacije

> **Napomena:** Ispod je i verzija na engleskom jeziku.

---

## Sažetak

Ovaj repozitorij sadrži skripte potrebne za prikupljanje, čišćenje, spajanje i vizualizaciju naučnih publikacija autora povezanih sa Bosnom i Hercegovinom. **Glavni izvori** podataka su [Akademski imenik](https://www.akademskiimenik.ba) i [OpenAlex](https://openalex.org/) repozitoriji. S obzirom da oba izvora podataka automatski skupljaju podatke te koriste mašinsko učenje za određene klasifikacije, greške u pojedinačnim podacima su moguće, međutim opći trendovi ne bi trebali biti značajno drugačiji.

Kod za preuzimanje, obradu i web aplikaciju nalazi se u ovom repozitoriju (vidi niže **Raspored skripti** i **Pokretanje**).

---

## Sadržaj

- [📚 BH naučne publikacije](#-bh-naučne-publikacije)
  - [Sažetak](#sažetak)
  - [Sadržaj](#sadržaj)
  - [Publikacije](#publikacije)
  - [Struktura podataka](#struktura-podataka)
  - [Objašnjenje skripti](#objašnjenje-skripti)
  - [Lokаlnо pokretanje](#lokаlnо-pokretanje)
  - [Streamlit aplikacija](#streamlit-aplikacija)
  - [Kompresija i split velikih fajlova](#kompresija-i-split-velikih-fajlova)
  - [Poznate manjkavosti i napomene](#poznate-manjkavosti-i-napomene)
  - [TODO](#todo)
  - [Licenca i zahvale](#licenca-i-zahvale)
    - [OpenAlex](#openalex)
    - [Semantic Scholar (API)](#semantic-scholar-api)
- [BH Scientific Publications](#bh-scientific-publications)
  - [Summary](#summary)
  - [Contents](#contents)
  - [Publications](#publications)
  - [Data Structure](#data-structure)
  - [Script Explanation](#script-explanation)
  - [Local Execution](#local-execution)
  - [Streamlit Application](#streamlit-application)
  - [Compression and Splitting of Large Files](#compression-and-splitting-of-large-files)
  - [Known Limitations and Notes](#known-limitations-and-notes)
  - [TODO](#todo-1)
  - [License and Acknowledgments](#license-and-acknowledgments)
    - [OpenAlex](#openalex-1)
    - [Semantic Scholar (API)](#semantic-scholar-api-1)

---

## Publikacije

Dataset je sačinjen od:

- Svih publikacija na BH akademskom imeniku;
- Svih publikacija autora u OpenAlex bazi koji su **u nekom trenutku karijere** imali afilijaciju u BiH.

Na taj način, sakupljeno je oko **110,000 radova**. 

## Struktura podataka

Većina kolona prati OpenAlex API, a nekoliko je specifično za radove iz BH akademskog imenika. 

**Kolone od interesa:**

- `id` – link na OpenAlex stranicu rada
- `authorships.author.display_name` – *array* imena autora  
- `authorships.author.id` – *array* OpenAlex ID-eva autora  
- `authorships.author.orcid` – *array* ORCID ID-eva  
- `authorships.author_position` – *array* pozicija autora (`first`, `mid`, `last`)  
- `authorships.countries` – *array* ISO kodova zemalja autora  
- `authorships.institutions.country_code` – *array* ISO kodova zemalja institucija  
- `authorships.institutions.display_name` – *array* naziva institucija  
- `authorships.institutions.id` – *array* ID/URL institucija  
- `cited_by_count` – broj citata  
- `display_name` – naziv rada
- `locations.source.issn` – *array* ISSN-ova  
- `primary_location.source.display_name,` – naziv časopisa/konferencije/izvora  
- `primary_location.source.type` – tip izvora: `journal`, `conference`, `submitted_version`…  
- `publication_year` – godina publikacije  
- `topics.domain.display_name` – *array* domena (najširi nivo)  
- `topics.field.display_name` – *array* polja (drugi nivo)  
- `topics.subfield.display_name` – *array* podpolja (treći nivo)  
- `topics.display_name` – *array* tema (četvrti nivo)  
- `type_crossref` – tip prema Crossref/alternativnim klasifikacijama  
- `semantic_id` – Semantic Scholar ID (za radove samo na Imeniku)  
- `abstract` – sažetak (za dio radova iz Imenika)  
- `addedViaImenik` – indikator da je rad došao iz Imenika  
- `scimagoRank` – Scimago rang (`Q1–Q4`, `-` = nerangiran u toj godini, `NaN` = nije u Scimago-u te godine)  
- `jHindex` – H-index časopisa (samo za rangirane)
- `coreRank` – CORE rang AI i ML konferencija (TODO)

> **Napomena za coreRank:** Ne postoji jedinstveni ID za konferencije kao što ima ISSN za časopise tako da je uparivanje rađeno po imenu/akronimu. Različiti nazivi za istu konferenciju mogu dovesti do propuštenih podudaranja.

---

## Objašnjenje skripti

1. `src/annotate_research_areas.py`: Dodaje istraživačke oblasti publikacijama koristeći openAI API i OpenAlex postojeću klasifikaciju.
2. `src/assign_publication_rankings_csv.py`: Dodjeljuje rangiranje publikacijama koristeći CSV fajlove za rangiranje.
3. `src/create_imenik_publications_via_database.py`: Kreira CSV fajl na osnovu baze podatka imenik publikacija bez dupliranja sa OpenAlex publikacijama. Potreban pristup bazi Imenik bazi podataka.
4. `src/data_compression.py`: Kompresuje velike CSV ili Parquet fajlove u manje Parquet fajlove.
5. `src/download_openalex_authors.py`: Preuzima podatke o autorima sa OpenAlex platforme.
6. `src/download_openalex_publications_via_authors.py`: Preuzima publikacije sa OpenAlex platforme na osnovu autora.
7. `src/find_imenik_publication_oa_variant.py`: Pronalazi OpenAlex varijante Imenik publikacija.
8. `src/join_oa_imenik_publications.py`: Spaja publikacije iz Imenika sa podacima iz OpenAlex-a.
9. `src/merge_core_rankings.py`: Spaja rangiranja konferencija iz više godina u jednu CSV fajl.
10. `src/merge_scimago_rankings.py`: Spaja SCImago rangiranja časopisa iz više godina u jednu CSV fajl.
11. `src/remove_duplicates_via_ids.py`: Uklanja duplikate publikacija na osnovu non-OpenAlex ID-ova.
12. `src/remove_nonuniqe_row.py`: Uklanja redove koji nisu jedinstveni iz CSV fajla.
13. `src/transform_imenik2oa_csv.py`: Transformiše CSV fajl iz Imenika u format OpenAlex-a.

---

## Lokаlnо pokretanje

**Instalacija potrebnih biblioteka**

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip-compile --resolver=backtracking --generate-hashes -o requirements.txt requirements.in
pip install -r requirements.txt
```

**Kreiranje database-a**

```bash
python src/download_openalex_bih_authors.py --output=data/ba_oa_authors_raw.csv
python src/openalex_publications_via_authors.py --authors_csv data/ba_oa_authors_raw.csv --output data/ba_oa_publications_raw.csv --email=email@email.ba
python src/remove_nonuniqe_row.py --input=data/ba_oa_publications_raw.csv --output data/ba_oa_publications2.csv --column id
python src/create_imenik_publications_via_database.py --openalex_csv data/ba_oa_publications.csv --output_csv=data/ba_imenik_publications_raw.csv
python src/transform_imenik2oa_csv.py --input data/ba_imenik_publications_raw.csv --output data/ba_imenik_publications.csv
python src/join_oa_imenik_publications.py --openalex data/ba_oa_publications2.csv --imenik data/ba_imenik_publications.csv --output data/ba_publications.csv
python src/find_imenik_publication_oa_variant.py --csv data/ba_publications.csv --email email@email.ba --output data/ba_publications.csv
python src/remove_duplicates_via_ids.py --csv data/ba_publications.csv --originals data/ba_publications.csv --duplicates data/ba_publications_dupl.csv
python src/annotate_research_areas.py --input data/ba_publications.csv --output data/ba_publications.csv 
python src/assign_publication_rankings_csv.py --journal_csv data/ranking_journals/SCIMAGOJR.csv --conference_csv data/ranking_conferences/CORE.csv --publications_csv data/ba_publications.csv --output_csv data/ba_publications.csv --issn_column locations.source.issn --venue_column location.source.display_name --verbose --mode both --year_column publication_year
```

**Napomena**
Pošto korake za pokreranje skripti radim retroaktivno, moguće da će biti nekih problema. Možete dignuti issue. Za testiranje bi bilo *pametno* koristiti različita imena input i output fajlova te koristiti verbose flag.

---

## Streamlit aplikacija

Pokretanje lokalno:

```bash
streamlit run streamlit_app.py
```
Streamlit je podešen da koristi Paraquet sharodove u `data/compressed` folderu jer to bio način da se čitava baza uploaduje na github, međutim za lokalno pokretanje moguće je koristiti originalni CSV file.

Aplikacija nudi filtriranje po godinama, Scimago rangu (samo časopisi), domenama/poljima/podpoljima, institucijama, autorima, te opciju **„prvi autor BA“**.

---

## Kompresija i split velikih fajlova

Zbog GitHub limita od 100 MB fajlove dijelimo u Parquet shardove ≤ **95 MB**.

Primjer:

```bash
python tools/split_parquet_by_size.py   --input data/ba_publications.csv   --output data/compressed
```
---

## Poznate manjkavosti i napomene

- Nije ulaženo u način na koji je OpenAlex određivao koji radovi pripadaju kojem autoru te način na koji je određivao afilijacije autora.
- Dio radova iz **BH akademskog imenika** nema sve mapirane OpenAlex kolone pošto je su oni odarađivali mnogo opsežniju AI identifikaciju, što za posljedicu može imati dosta `NaN` kolona.  
- **Scimago:** `-` = nerangiran u toj godini; `NaN` = uopće nije u Scimago-u rankingu te godine.  
- **Konferencije:** uparivanje po imenu/akronimu može biti nepouzdano s toga u vizuelizaciji nije prikazano ništa vezano za te podatke.

---

## TODO

- [ ] Poboljšati normalizaciju i detekciju konferencija.
- [ ] Deduplikacija i validacija metapodataka.
- [ ] Kreirati jednu biblioteku za često korištene funkcije i čišćenje koda.
- [ ] Unifikacija koda za logove.

---

## Licenca i zahvale

- Licenca: **CC BY 4.0** 
- Zahvale timu [**Akademski imenik BiH**](https://akademskiimenik.ba/) [**OpenAlex**](https://openalex.org/), [**Semantic Scholar**](https://www.semanticscholar.org/) na davanju pristupa podacima i velikim trudu da podaci o naučno-istraživačkom radu budu javno dostupni.

### OpenAlex
Podaci iz **OpenAlex** dostupni su pod licencom **CC0 1.0 (Public Domain Dedication)**.
> Priem, J., Piwowar, H., & Orr, R. (2022). *OpenAlex: A fully-open index of scholarly works, authors, venues, institutions, and concepts.* arXiv. https://arxiv.org/abs/2205.01833

### Semantic Scholar (API)

Ovaj projekat koristi **Semantic Scholar API (AI2)** u skladu s uslovima licence za **internu, nekomercijalnu** upotrebu u svrhe istraživanja/obrazovanja.

[![Semantic Scholar](./a2.png "Semantic Scholar (AI2)")]
(https://www.semanticscholar.org/?utm_source=api)

Podaci djelimično pruženi od **Semantic Scholar (AI2)**.

---

# BH Scientific Publications

> **Note:** Machine translated.

---

## Summary

This repository contains scripts necessary for collecting, cleaning, merging, and visualizing scientific publications of authors associated with Bosnia and Herzegovina. **The main data sources** are the [Academic Directory](https://www.akademskiimenik.ba) and [OpenAlex](https://openalex.org/) repositories. Since both data sources automatically collect data and use machine learning for certain classifications, errors in individual data entries are possible; however, general trends should not differ significantly.

The code for downloading, processing, and the web application is located in this repository (see below **Script Overview** and **Running**).

---

## Contents

- [📚 BH naučne publikacije](#-bh-naučne-publikacije)
  - [Sažetak](#sažetak)
  - [Sadržaj](#sadržaj)
  - [Publikacije](#publikacije)
  - [Struktura podataka](#struktura-podataka)
  - [Objašnjenje skripti](#objašnjenje-skripti)
  - [Lokаlnо pokretanje](#lokаlnо-pokretanje)
  - [Streamlit aplikacija](#streamlit-aplikacija)
  - [Kompresija i split velikih fajlova](#kompresija-i-split-velikih-fajlova)
  - [Poznate manjkavosti i napomene](#poznate-manjkavosti-i-napomene)
  - [TODO](#todo)
  - [Licenca i zahvale](#licenca-i-zahvale)
    - [OpenAlex](#openalex)
    - [Semantic Scholar (API)](#semantic-scholar-api)
- [BH Scientific Publications](#bh-scientific-publications)
  - [Summary](#summary)
  - [Contents](#contents)
  - [Publications](#publications)
  - [Data Structure](#data-structure)
  - [Script Explanation](#script-explanation)
  - [Local Execution](#local-execution)
  - [Streamlit Application](#streamlit-application)
  - [Compression and Splitting of Large Files](#compression-and-splitting-of-large-files)
  - [Known Limitations and Notes](#known-limitations-and-notes)
  - [TODO](#todo-1)
  - [License and Acknowledgments](#license-and-acknowledgments)
    - [OpenAlex](#openalex-1)
    - [Semantic Scholar (API)](#semantic-scholar-api-1)

---

## Publications

The dataset consists of:

- All publications in the BH Academic Directory;
- All publications of authors in the OpenAlex database who **at some point in their career** had an affiliation in BiH.

In this way, approximately **110,000 works** were collected.

## Data Structure

Most columns follow the OpenAlex API, and a few are specific to works from the BH Academic Directory.

**Columns of Interest:**

- `id` – link to the OpenAlex page of the work
- `authorships.author.display_name` – *array* of author names  
- `authorships.author.id` – *array* of OpenAlex author IDs  
- `authorships.author.orcid` – *array* of ORCID IDs  
- `authorships.author_position` – *array* of author positions (`first`, `mid`, `last`)  
- `authorships.countries` – *array* of ISO country codes of authors  
- `authorships.institutions.country_code` – *array* of ISO country codes of institutions  
- `authorships.institutions.display_name` – *array* of institution names  
- `authorships.institutions.id` – *array* of institution IDs/URLs  
- `cited_by_count` – number of citations  
- `display_name` – title of the work
- `locations.source.issn` – *array* of ISSNs  
- `primary_location.source.display_name,` – name of the journal/conference/source  
- `primary_location.source.type` – source type: `journal`, `conference`, `submitted_version`…  
- `publication_year` – year of publication  
- `topics.domain.display_name` – *array* of domains (broadest level)  
- `topics.field.display_name` – *array* of fields (second level)  
- `topics.subfield.display_name` – *array* of subfields (third level)  
- `topics.display_name` – *array* of topics (fourth level)  
- `type_crossref` – type according to Crossref/alternative classifications  
- `semantic_id` – Semantic Scholar ID (for works only in the Directory)  
- `abstract` – abstract (for some works from the Directory)  
- `addedViaImenik` – indicator that the work came from the Directory  
- `scimagoRank` – Scimago rank (`Q1–Q4`, `-` = unranked in that year, `NaN` = not in Scimago that year)  
- `jHindex` – H-index of the journal (only for ranked ones)
- `coreRank` – CORE rank of AI and ML conferences (TODO)

> **Note on coreRank:** There is no unique ID for conferences like ISSN for journals, so matching was done by name/acronym. Different names for the same conference may lead to missed matches.

---

## Script Explanation

1. `src/annotate_research_areas.py`: Adds research areas to publications using the OpenAI API and OpenAlex's existing classification.
2. `src/assign_publication_rankings_csv.py`: Assigns rankings to publications using ranking CSV files.
3. `src/create_imenik_publications_via_database.py`: Creates a CSV file based on the Directory database without duplicating OpenAlex publications. Requires access to the Directory database.
4. `src/data_compression.py`: Compresses large CSV or Parquet files into smaller Parquet files.
5. `src/download_openalex_authors.py`: Downloads author data from the OpenAlex platform.
6. `src/download_openalex_publications_via_authors.py`: Downloads publications from the OpenAlex platform based on authors.
7. `src/find_imenik_publication_oa_variant.py`: Finds OpenAlex variants of Directory publications.
8. `src/join_oa_imenik_publications.py`: Merges publications from the Directory with data from OpenAlex.
9. `src/merge_core_rankings.py`: Merges conference rankings from multiple years into one CSV file.
10. `src/merge_scimago_rankings.py`: Merges SCImago journal rankings from multiple years into one CSV file.
11. `src/remove_duplicates_via_ids.py`: Removes duplicate publications based on non-OpenAlex IDs.
12. `src/remove_nonuniqe_row.py`: Removes non-unique rows from a CSV file.
13. `src/transform_imenik2oa_csv.py`: Transforms a CSV file from the Directory into OpenAlex format.

---

## Local Execution

**Installing Required Libraries**

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip-compile --resolver=backtracking --generate-hashes -o requirements.txt requirements.in
pip install -r requirements.txt
```

**Creating the Database**

```bash
python src/download_openalex_bih_authors.py --output=data/ba_oa_authors_raw.csv
python src/openalex_publications_via_authors.py --authors_csv data/ba_oa_authors_raw.csv --output data/ba_oa_publications_raw.csv --email=email@email.ba
python src/remove_nonuniqe_row.py --input=data/ba_oa_publications_raw.csv --output data/ba_oa_publications2.csv --column id
python src/create_imenik_publications_via_database.py --openalex_csv data/ba_oa_publications.csv --output_csv=data/ba_imenik_publications_raw.csv
python src/transform_imenik2oa_csv.py --input data/ba_imenik_publications_raw.csv --output data/ba_imenik_publications.csv
python src/join_oa_imenik_publications.py --openalex data/ba_oa_publications2.csv --imenik data/ba_imenik_publications.csv --output data/ba_publications.csv
python src/find_imenik_publication_oa_variant.py --csv data/ba_publications.csv --email email@email.ba --output data/ba_publications.csv
python src/remove_duplicates_via_ids.py --csv data/ba_publications.csv --originals data/ba_publications.csv --duplicates data/ba_publications_dupl.csv
python src/annotate_research_areas.py --input data/ba_publications.csv --output data/ba_publications.csv 
python src/assign_publication_rankings_csv.py --journal_csv data/ranking_journals/SCIMAGOJR.csv --conference_csv data/ranking_conferences/CORE.csv --publications_csv data/ba_publications.csv --output_csv data/ba_publications.csv --issn_column locations.source.issn --venue_column location.source.display_name --verbose --mode both --year_column publication_year
```

**Note**
Since the steps for running scripts are being done retroactively, there may be some issues. You can raise an issue. For testing, it would be *smart* to use different names for input and output files and use the verbose flag.

---

## Streamlit Application

Run locally:

```bash
streamlit run streamlit_app.py
```
Streamlit is set to use Parquet shards in the `data/compressed` folder because that was the way to upload the entire database to GitHub. However, for local execution, it is possible to use the original CSV file.

The application offers filtering by years, Scimago rank (journals only), domains/fields/subfields, institutions, authors, and the option **"first author BA"**.

---

## Compression and Splitting of Large Files

Due to GitHub's 100 MB limit, files are split into Parquet shards ≤ **95 MB**.

Example:

```bash
python tools/split_parquet_by_size.py   --input data/ba_publications.csv   --output data/compressed
```
---

## Known Limitations and Notes

- No effort was made to determine how OpenAlex identified which works belong to which author and how it determined author affiliations.
- Some works from the **BH Academic Directory** do not have all mapped OpenAlex columns since they performed much more extensive AI identification, which may result in many `NaN` columns.  
- **Scimago:** `-` = unranked in that year; `NaN` = not in Scimago ranking that year.  
- **Conferences:** matching by name/acronym may be unreliable, so nothing related to these data is displayed in the visualization.

---

## TODO

- [ ] Improve normalization and detection of conferences.
- [ ] Deduplication and validation of metadata.
- [ ] Create a single library for frequently used functions and code cleaning.
- [ ] Unify code for logs.

---

## License and Acknowledgments

- License: **CC BY 4.0** 
- Thanks to the team at [**Akademski imenik BiH**](https://akademskiimenik.ba/) [**OpenAlex**](https://openalex.org/), [**Semantic Scholar**](https://www.semanticscholar.org/) for providing access to data and their great effort to make data on scientific research publicly available.

### OpenAlex
Data from **OpenAlex** are available under the **CC0 1.0 (Public Domain Dedication)** license.
> Priem, J., Piwowar, H., & Orr, R. (2022). *OpenAlex: A fully-open index of scholarly works, authors, venues, institutions, and concepts.* arXiv. https://arxiv.org/abs/2205.01833

### Semantic Scholar (API)

This project uses the **Semantic Scholar API (AI2)** in accordance with the license terms for **internal, non-commercial** use for research/educational purposes.

[![Semantic Scholar](./a2.png "Semantic Scholar (AI2)")](https://www.semanticscholar.org/?utm_source=api)

Data partially provided by **Semantic Scholar (AI2)**.

---
