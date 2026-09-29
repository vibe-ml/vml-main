# OpenAlex Abstract Ingestion & Reconstruction Samples

Sample records extracted from derived Parquet file:  
`data/openalex/derived/015d321b6357b8d35523298ed3b54f9c6bcf8309b5d1d9e1c2f06757dbb7b582.parquet`

Parquet table schema:
- `entity_id` (`VARCHAR`): OpenAlex entity URI (e.g. `https://openalex.org/W...`)
- `version_id` (`VARCHAR`): SHA-256 version identity hash
- `payload` (`JSON`): Complete record payload including reconstructed abstract

---

## Sample Rows

### Row 1

- **Entity ID:** `https://openalex.org/W7214202339`
- **Version ID:** `8c221fc364a3651fe0e37b7650540f5aef2fdc6b8d4b0e903af6c839a43d9f52`
- **Title:** K3s Solution Catalog for ISO/IEC 42001-Compliant Industrial AI Systems
- **Publication Date:** `2026-09-25`
- **DOI:** `https://doi.org/10.5281/zenodo.22947711`
- **Primary Topic:** None (Field: None)
- **Reconstructed Plain-Text Abstract:**
  > Companion catalog to the doctoral thesis Automatización de operaciones en el ciclo de vida de soluciones para fabricación cero defectos (Mateo-Casalí, Universitat Politècnica de València, 2026). Organises K3s MLOps solutions by deployment tier (Edge, Platform, Enterprise) and functional category, with deployment questionnaires and explicit mappings to ISO/IEC 42001 Annex B requirements. Version 2.0.0 contains 30 Helm charts, a phased installer, zone NetworkPolicies, an API audit policy, a Helm repository built with its dependencies, and the laboratory validation of the catalog (report, runner and raw evidence in docs/lab-validation/).

---

### Row 2

- **Entity ID:** `https://openalex.org/W7214202647`
- **Version ID:** `8499aed1409e49ed38ca5d9779896f792328e1769cb9cc97d175c07368537d9f`
- **Title:** Analisis Konfirmasi Bank dan Prosedur Alternatif sebagai Bukti Audit atas Kas dan Setara Kas pada KAP XYZ
- **Publication Date:** `2026-09-25`
- **DOI:** `https://doi.org/10.61083/ebisma.v6i3.211`
- **Primary Topic:** School Leadership and Teacher Performance (Field: Social Sciences)
- **Reconstructed Plain-Text Abstract:**
  > Audit atas kas dan setara kas memerlukan bukti audit yang cukup dan tepat untuk mendukung penilaian auditor atas kewajaran saldo yang disajikan dalam laporan keuangan. Artikel ini bertujuan untuk menganalisis peran konfirmasi bank dan prosedur alternatif sebagai bukti audit atas akun kas dan setara kas pada KAP XYZ. Salah satu prosedur yang umum digunakan untuk memperoleh bukti audit adalah konfirmasi bank, namun dalam praktiknya auditor dapat menghadapi kondisi ketika balasan konfirmasi tidak diperoleh sehingga diperlukan prosedur audit alternatif. Artikel ini menggunakan pendekatan kualitatif deskriptif dengan teknik pengumpulan data melalui observasi, dokumentasi, dan wawancara terhadap satu senior auditor dan tiga junior auditor. Hasil observasi dan wawancara menunjukkan bahwa konfirmasi bank merupakan sumber bukti audit yang memiliki tingkat keandalan tinggi karena informasi diperoleh langsung dari pihak ketiga yang independen. Ketika balasan konfirmasi tidak diperoleh, auditor menerapkan prosedur alternatif untuk memperoleh bukti audit yang memadai. Hasil pembahasan menunjukkan bahwa prosedur alternatif dapat memberikan bukti audit yang memadai sepanjang bukti yang diperoleh relevan dan andal. Konfirmasi bank dan prosedur alternatif memiliki peran yang saling melengkapi dalam mendukung pemerolehan bukti audit yang cukup dan tepat sehingga auditor dapat menarik kesimpulan dan memberikan opini atas laporan keuangan secara memadai.

---

### Row 3

- **Entity ID:** `https://openalex.org/W7214202949`
- **Version ID:** `741a579839977fa19718a4be0b31a8c78ecf5df516a2e58113e79ed8066570bb`
- **Title:** A STABILIZED HIRUDIN SOLID PREMIX IN THE TRANSLATIONAL CONTEXT OF CORONARY THROMBOSIS AND ACUTE CORONARY ISCHEMIA
- **Publication Date:** `2026-09-25`
- **DOI:** `https://doi.org/10.61796/jhsm.v1i4.39`
- **Primary Topic:** Acute Myocardial Infarction Research (Field: Medicine)
- **Reconstructed Plain-Text Abstract:**
  > Objective: Persistent thrombin activity contributes to coronary thrombosis and recurrent ischemia in acute coronary syndromes. We measured hirudin activity in a low-water-activity premix and reviewed randomized evidence from unstable angina and non-ST-elevation acute coronary syndromes. Method: Optimized BCLC® formulations retained 90.6-92.1% antithrombin activity at water activity values of 0.26-0.34. High-humidity processing produced the highest water activity (0.49) and lowest retention (72.9%); without citrate buffer, retention was 80.1%. An angiographic unstable-angina trial reported dose-related antithrombotic effects of recombinant hirudin. In GUSTO-IIb, 12,142 patients were randomized, and death or myocardial infarction at 24 h occurred in 1.3% with hirudin and 2.1% with heparin. OASIS-2 enrolled 10,141 patients with non-ST-elevation acute myocardial ischemia. Results: At 7 days, cardiovascular death or new myocardial infarction occurred in 3.6% with hirudin and 4.2% with heparin; treatment-period analyses favored hirudin, but major bleeding requiring transfusion was higher. These trials establish both pharmacodynamic activity and a dose-safety context. Novelty: The premix data permit oral exposure and coagulation biomarkers to be examined from a defined activity level.

---

## Original Inverted Abstract (Row 1)

In the upstream OpenAlex raw data and database (`raw.openalex_work_versions`), the abstract for Row 1 is stored in `payload->'abstract_inverted_index'` as a token-to-position mapping:

```json
{
  "Companion": [0],
  "catalog": [1, 79],
  "to": [2, 45],
  "the": [3, 74, 78],
  "doctoral": [4],
  "thesis": [5],
  "Automatización": [6],
  "de": [7, 12, 14, 23],
  "operaciones": [8],
  "en": [9],
  "el": [10],
  "ciclo": [11],
  "vida": [13],
  "soluciones": [15],
  "para": [16],
  "fabricación": [17],
  "cero": [18],
  "defectos": [19],
  "(Mateo-Casalí,": [20],
  "Universitat": [21],
  "Politècnica": [22],
  "València,": [24],
  "2026).": [25],
  "Organises": [26],
  "K3s": [27],
  "MLOps": [28],
  "solutions": [29],
  "by": [30],
  "deployment": [31, 40],
  "tier": [32],
  "(Edge,": [33],
  "Platform,": [34],
  "Enterprise)": [35],
  "and": [36, 42, 73, 82],
  "functional": [37],
  "category,": [38],
  "with": [39, 70],
  "questionnaires": [41],
  "explicit": [43],
  "mappings": [44],
  "ISO/IEC": [46],
  "42001": [47],
  "Annex": [48],
  "B": [49],
  "requirements.": [50],
  "Version": [51],
  "2.0.0": [52],
  "contains": [53],
  "30": [54],
  "Helm": [55, 67],
  "charts,": [56],
  "a": [57, 66],
  "phased": [58],
  "installer,": [59],
  "zone": [60],
  "NetworkPolicies,": [61],
  "an": [62],
  "API": [63],
  "audit": [64],
  "policy,": [65],
  "repository": [68],
  "built": [69],
  "its": [71],
  "dependencies,": [72],
  "laboratory": [75],
  "validation": [76],
  "of": [77],
  "(report,": [80],
  "runner": [81],
  "raw": [83],
  "evidence": [84],
  "in": [85],
  "docs/lab-validation/).": [86]
}
```

### Reconstruction Process
During `rebuild_job` (`src/ingestion/snapshot.py:derived`):
1. Pairs `(position, word)` are unpacked: `(0, "Companion")`, `(1, "catalog")`, `(2, "to")`, ..., `(86, "docs/lab-validation/).")`.
2. Sorted by position ascending: `0, 1, 2, ..., 86`.
3. Joined with spaces into the plain text string stored in `payload->'abstract'`.
