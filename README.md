## Scala and Spark basics Capstone Project: Advanced JSON Manipulation using Spark on Databricks
### Objective
The overarching goal is to harness Scala's with spark potential for data manipulation and handling large datasets. 
Will craft an extensive JSON database, wrangle this data, and subsequently store the data in a medallion Architecture, also storing as CSV for further analysis outside of Databrick.

### Phase 01: Generating a Comprehensive Fake JSON Database, store it to DBFS
- **Objective**: Fabricate a voluminous JSON-format database filled with diverse structured data.
- **Specifications**: 
    - Dataset should comprise **10,000 records**.
    - Use libraries like [json-generator](https://www.json-generator.com/) or any similar tool for data generation.
- **Structure**:
    - `id`: Unique identifier (integer).
    - `name`: Full name (string).
    - `email`: Email address (string).
    - `dob`: Date of birth (string, format: "YYYY-MM-DD").
    - `is_active`: Boolean indicating if the user is active.
    - `joined_date`: The date on which they joined (string, format: "YYYY-MM-DD").
    - `address`: 
        - `street`: Street name (string).
        - `city`: City name (string).
        - `state`: State name (string).
        - `zipcode`: Zip code (string).
        - `country`: Country name (string).
    - `phoneNumbers`: Array of phone numbers (strings).
    - `website`: Personal website URL (string).
    - `tags`: Array of associated labels or interests (strings).
    - `employment`: 
        - `job_title`: Title of the job (string).
        - `department`: Department of employment (string).
        - `company`: Company name (string).
- **Store Data**: Store the Json on DBFS

### Phase 02: Deft JSON Data Handling in Scala
- **Objective**: Load the hefty JSON, execute a suite of transformations, and archive the outcomes into a CSV format.
- **Transformations**:
    1. **Filtering**: 
        - Cull records for users who joined after a particular date.
        - Isolate users based on specific tags, e.g., "developer".
        - Pick out users based on their active status.
    2. **Data Flattening**: 
        - De-nest the `address` and `employment` structures to a flat structure.
    3. **Data Enrichment**:
        - Craft a `location` field by melding `city`, `state`, and `country`.
        - Compute and introduce a `phoneCount` field delineating the count of phone numbers each user possesses.
    4. **Textual Alterations**: 
        - Transform email addresses into lowercase.
        - Craft a `nameInitials` field, representing the initials of user names.
    5. **Array-based Tasks**:
        - Spotlight users boasting more than one phone number.
        - Morph the array of `tags` into a solitary comma-separated string.
- **Output**: 
    - Warehouse the manipulated data as a CSV file with columns such as `id`, `name`, `email`, `street`, `city`, `zipcode`, `phoneNumbers`, and any novel columns you've injected.
