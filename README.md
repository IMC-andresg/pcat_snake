# About The Snake Tool

Snake is Part of the Microsoft Product Catalogue Automation Tools (PCAT) suite of automation tools. The Snake wraps itself around (get it?) the complex dataset from Microsoft, and performs a data transformation to assess and interpret the raw data to create a version that is more suitable for use by real-world Providers.  Its scope includes:

* Using ISV's Geographic Availability Data to build per-Provider Catalogues based on their geographic locale
* Fleshes out all of the Offer Relationships to turn the Flat Catalogue into a Depth Catalogue
    * Parent to Add-On Relationships
    * Add-On to Add-On-to-Add On Relationships
    * Add-Ons with Multiple Parent Relationships
    * Add-Ons with Multiple Qualification Relationships

Having a Depth Catalogue is important to Providers because they provision Active Subscriptions which are bound to relevant Add-Ons.  In CloudBlue, every permutation of relationship must be configured so that Partners are never stuck with an Active Subscription not supporting a compatible ISV Add-On.  This leads to a single ISV Add-On Offer being duplicated for each relationship type; a many-to-one relationship.

When the Snake is used to support creation of Platform Product Requirements (PPRs), it is clear to a human reader exactly how many times and where an Offer must be present in the Provider's instance of CloudBlue or other BSS.  PPRs act as a the binding human readable requirements documentation for platform configuration of a product.  They are used by :

* Ingram CSO and Datacom to understand the scope of configuration work to be done
* GCSPdM to verify correct configuration and authorize commercial go-live
* ISO-27001 Compliance Audits

For more information, please refer to the Sharepoint [page](https://ingrammicro.sharepoint.com/sites/GlobalCloudServicesProductManagement/SitePages/Channel-Knowledge-Base-for-Microsoft-Product-Catalogue-Automation-Tools-(PCAT).aspx#the-snake-product-catalogue-transformation-(pct)-tool)

# Prerequisites

* Python 3.7: please refer to platform specific [instructions](https://www.python.org/downloads/) on how to install python

# Installation

To install python dependencies:

```
pip install -r requirements.txt
```

# Usage

1. Initialize configuration: Copy the `config_template.json` to a new file `config.json` and add your `CONNECT_TOKEN` and any other environment specific changes.

2. Create / point to files directory: The tool expects a folder in the root directory "files" with the monthly offer matrices and previous output files of the tool. The folder should have at least two previous months of data to compare states. The paths of the files and names can be configured using the `config.json` file. 

3. Run the tool:
```
python SnakeTool.py
```

## Local DB

The tool stores answers for SKU groups and shortnames so that you don't have to answer them again in subsequent runs. To remove the saved answers, remove the db.json file. You can also inspect this file and make changes it as needed. 

## Loggin

The tool generates a log file for each run. Any errors will be logged to this file. The file name contains the current timestamp. Example: `snake_20210107-173001.log`. 