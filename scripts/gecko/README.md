# About The Gecko Tool

Gecko is a custom python script used to generate PPR files for NCE Perpetual and Software Subscriptions products.
It uses template excel files to generate the PPR based on Connect items for those products. Gecko uses the Connect API to download a list of SKUs to populate the PPRs.

Gecko asks for the following information to generate the PPRs:

1. Project name (Example: acme)
2. Select country from a list
3. Select which product from a list (perpetual or software)

Gecko generates two files, L1 and L0 under the out folder.

# PPR Templates

As the PPR templates get updated, the code needs to be modified to use the new columns. Please use config.json to indicate the name of the template file to use for each product.

# Prerequisites

* Python 3.7: please refer to platform specific [instructions](https://www.python.org/downloads/) on how to install python

# Installation

1. Create a Python virtual environment:
```
python -m venv env
.\env\Scripts\activate
```

2. Install python dependencies:

```
pip install -r requirements.txt
```

# Setup & Run

1. Initialize configuration: Copy the `config_template.json` to a new file `config.json` and add your `CONNECT_TOKEN` and any other environment specific changes.

2. Run the tool:
```
python gecko.py
```