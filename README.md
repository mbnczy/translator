
---

# 📘 Translator

**Translator** is a multiprocessing-aware translation pipeline designed to translate large CSV files efficiently using [EasyNMT](https://github.com/UKPLab/EasyNMT). It parallelizes translation using subprocesses and monitors them using a watchdog thread that ensures fault-tolerance, logs progress, and automatically recovers from crashes.

---

## 🚀 Features

* 🔁 **Parallel Translation** using multiple subprocesses.
* 🧠 **Watchdog** thread monitors and restarts failed subprocesses.
* 💥 **Crash Recovery** with resume capability.
* 📝 **Configurable Logging** and regular progress reports.
* 🧪 **Validation** of translation outputs.
* 🧹 **Postprocessing** for cleaning and finalizing translations.
* ⚙️ **Easy configuration** via JSON.

---

## 📦 Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/yourusername/translator.git
cd translator/src
pip install -r requirements.txt
```

---

## ⚙️ Configuration

Set up your job using the __`config.json`__ config file:

| Key                      | Description                                            |
| ------------------------ | ------------------------------------------------------ |
| `data_path`              | Input CSV file path                                    |
| `delimiter`              | Delimiter used in the CSV                              |
| `source_lang`            | Source language (e.g., `"cs"`)                         |
| `target_lang`            | Target language (e.g., `"en"`)                         |
| `num_chunks`             | Number of subprocesses for parallel translation        |
| `column_name`            | Column name containing text to translate               |
| `translated_column_name` | Name of the column to store translations               |
| `row_start`/`row_end`    | Optionally define row range (use `-1` to process all)  |
| `write_step`             | Frequency of saving intermediate results               |
| `active_logging_minutes` | Time window to consider a process active               |
| `log_interval`           | Interval between logs (in minutes)                     |
| `patience`               | Number of missed intervals before restarting a process |

---

## 🛠️ Usage

Using **Translator** is simple. Once the configuration file is ready, just run:

```bash
python3 main.py
```

No additional command-line arguments needed.

---

## 🧪 Validation & Postprocessing

After translation:

* The outputs are **validated** to ensure quality and completeness.
* A set of **postprocessing** steps refines the translations (e.g., whitespace trimming, filtering invalid data).

---

## 📝 License

Licensed under the **MIT License**.

---
