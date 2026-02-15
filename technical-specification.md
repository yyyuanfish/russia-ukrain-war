# Structure of the whole project
1. input files:
* config.json 
2. codes:
* harvest_wikidata.py
* harvest_navboxes.py
* harvest_categories.py
* attribution.py
* [visualization.py]
3. output files:
* classified_entities.jsonl


## 1. Input files

`config.json`: stores parameter for our search and attribution of the entities. 
Specifically, it defines the countries and languages of interest.

The information here is manually arranged by a researcher.


Should look something like:

```json
{
  "conflicting_parties": {
    "party1": {
        "ID": "Q159", # Wikidata ID of the first country, in our case, Russia
        "allies": ["Q114334914", "Q114327408", "Q16912926", "Q15925436"] # Wikidata IDs of the allies/dependent sides/"proxies", in this case, DPR, LPR, Novorossia, Crimean Republic
    }, 
    "party2": {
        "ID": "Q159", # Similar to party1, in our case, Ukraine
        "allies": [] # allow for empty lists     
    }
  },
  "languages": { # lang codes for collecting the Wikidata descriptions
    "party1": "ru", # should correspond to the first country's language
    "party2": "uk", # should correspond to the second country's language
    "party3": "en" # corresponds to a language of the third party (not directly involved in conflict)
  },
  "navbox_names": ["Russo-Ukrainian war", "Russo-Ukrainian war (2022–present)"], # list of navbox titles that would be scraped for entities
  "category_names": ["Russian-Ukrainian war"] # list of categories that would be scraped for entities
}
```

## 2. Codes
* `harvest_wikidata.py`: takes information from all entities related to the Russian-Ukrainian war in Wikidata
* `harvest_navboxes.py`: takes the `navbox_names` from the config file and scrapes the corresponding Wikipedia pages for entities
* `harvest_categories.py`: takes the `category_names` from the config file and scrapes the corresponding Wikipedia pages for entities (breadth-first search)

For all three scripts, they should be run in the following way: 

```aiignore
python harvest_{wikidata, navboxes, categories}.py --config config.json --output data/entities/{wikidata, navboxes, categories}_entities.jsonl
```
, where `config.json` is the path to the config file, and `data/entities/{...}_entities.jsonl` is the path to the output file.


* `attribution.py`: assigns the countries to the entities based on the `conflicting_parties` in the config file. There are four possible values: `party1`, `party2`, `mixed` (when there is information about both sides in the same entity) and `other` (otherwise, i.e. when the entity is not related to either of the parties).

The run of this script should look like:

```aiignore
python attribution.py --config config.json --entities_folder data/entities --output data/classified_entities.jsonl
```

, where `data/entities` is the path to the folder with the output files from the previous step, and `data/classified_entities.jsonl` is the path to the output file.
