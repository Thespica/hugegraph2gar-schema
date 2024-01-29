```yaml
chunk_size: 2
label: person
prefix: person/
property_groups: 
  - file_type: csv
    prefix: city/
    properties: 
      - data_type: string
        is_primary: false
        name: city
  - file_type: csv
    prefix: name/
    properties: 
      - data_type: string
        is_primary: false
        name: name
  - file_type: csv
    prefix: age/
    properties: 
      - data_type: int32
        is_primary: false
        name: age
version: gar/v1

chunk_size: 2
label: software
prefix: software/
property_groups: 
  - file_type: csv
    prefix: price/
    properties: 
      - data_type: int32
        is_primary: false
        name: price
  - file_type: csv
    prefix: name/
    properties: 
      - data_type: string
        is_primary: false
        name: name
  - file_type: csv
    prefix: lang/
    properties: 
      - data_type: string
        is_primary: false
        name: lang
version: gar/v1

adj_lists: 
  - aligned_by: src
    file_type: csv
    ordered: false
    prefix: unordered_by_source/
    property_groups: 
      - file_type: csv
        prefix: date/
        properties: 
          - data_type: 
            is_primary: false
            name: date
      - file_type: csv
        prefix: weight/
        properties: 
          - data_type: double
            is_primary: false
            name: weight
chunk_size: 2
directed: false
dst_chunk_size: 2
dst_label: person
edge_label: knows
prefix: person_knows_person/
src_chunk_size: 2
src_label: person
version: gar/v1

adj_lists: 
  - aligned_by: src
    file_type: csv
    ordered: false
    prefix: unordered_by_source/
    property_groups: 
      - file_type: csv
        prefix: date/
        properties: 
          - data_type: 
            is_primary: false
            name: date
      - file_type: csv
        prefix: weight/
        properties: 
          - data_type: double
            is_primary: false
            name: weight
chunk_size: 2
directed: false
dst_chunk_size: 2
dst_label: software
edge_label: created
prefix: person_created_software/
src_chunk_size: 2
src_label: person
version: gar/v1

name: hugegraph
prefix: ./
version: gar/v1

```