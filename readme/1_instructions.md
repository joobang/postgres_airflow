# Overview
본 과제의 목표는 파트 1에서 제공해드린 Airflow 플랫폼 코드베이스와 DAG의 결함을 보하고 파트 2에서 협업이 용이하게끔 제시된 인터페이스를 구현하는 것입니다.

제공해드린 Airflow 코드베이스를 기반으로 구동되어있는 가상의 Airflow 플랫폼은 한명의 데이터 엔지니어가 혼자서 판단하기에 “working”하게끔 만들어둔 상태이며, 상황에 따라서 특정 DAG가 잘 동작하지 않거나, 이슈가 발생할 수 있는 여지들이 존재합니다.

제출하실 코드베이스는 하나이기 때문에 한번에 두 파트를 동시에 해결하는 것도 가능하며, 파트 1을 해결하면서 얻은 인사이트를 파트 2를 해결하는데 활용(vice-versa도 가능합니다) 하실 수도 있습니다.

따라서 각 파트에서 해결해야하는 문제들이 무엇인지를 모두 확인 한 뒤, 제공해드리는 코드베이스를 확인하시는 것을 추천드립니다.
# PART 1. 데이터 파이프라인 결함 보완하기

현재의 데이터 인프라 운영 조건에서 매출 마트 데이터의 정합성을 최대한 보장하기 위해서 ETL 파이프라인을 점검하고 발견되는 결함들을 보완해주세요. 점검이 필요한 항목은 다음과 같습니다:

- `postgres_merge_upsert_ad_lineitem`과 `postgres_merge_upsert_units` DAG가 원본 운영계 테이블과의 정합성을 보장하는 방식으로 ETL이 이루어지는지?

- `postgres_transform_load_m1_d_ad_mart_metrics` DAG의 Transform + Load가 **idempotent**하게 구현이 되어있는지?

- `postgres_transform_load_m1_d_ad_mart_metrics` DAG가 Fact와 Mart 데이터간 정합성을 보장할 수 있게끔 구현이 되어있는지?

각 결함의 유무와 보완의 단서를 찾기 위해서 readme/datasets.md를 꼼꼼히 확인해주세요.

# PART 2. 기존의 Airflow 환경을 셀프 서빙 플랫폼으로 업그레이드하기

전통적인 데이터 엔지니어 직군의 역할이 “Hadoop ecosystem상에서 Scala, Python 코드를 기반으로 Spark ETL 데이터 파이프라인 구현 및 운영”에 초점이 맞추어져 있었다면, 버즈빌을 포함한 많은 IT기업들 [analytics engineering](https://www.getdbt.com/what-is-analytics-engineering/) 개념을 도입하고 Airflow를 데이터 엔지니어만이 아니라 분석 엔지니어, 분석가, 머신러닝 엔지니어등이 같이 협업할 수 있는 셀프 서빙 플랫폼으로 활용하고 있습니다.

제공해드린 Airflow 코드베이스에 구현되어있는 DAG들의 주요 기능은 정해진 시간에 순서에 맞추어 분석계 Postgres상에서 ETL SQL을 실행하는 것입니다. 그렇기에, 기존 구현에서 큰 변화 없이도 SQL 구문과 몇 가지 configuration들만 작성하면 원하는 형태의 DAG가 만들어지는 인터페이스를 구성해서 셀프 서빙 플랫폼으로의 전환이 가능해집니다.

Part 2에서는 Python의 숙련도가 없는 Data Analyst도 분석 SQL과 YAML만으로도 DAG를 생성할 수 있도록 DAG Builder를 구현하는 것이 목표입니다.

제공해드린 postgres_merge_upsert 타입의 YAML 스펙(configs/configs/postgres_merge_upsert/README.md), 그리고 해당 스펙들에 맞추어 작성된 YAML 명세를 확인하시고 DAG Builder를 구현해주세요.

위 요구사항을 SaaS로 잘 구현한 플랫폼으로는 DBT가 있습니다. DBT에서 플랫폼 유저에게 어떤 인터페이스를 제공하는지를 참고해주시면 과제 수행에 도움이 될 수 있습니다.
