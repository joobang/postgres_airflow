# Boilerplates
기본적으로 제공해드리는 Boilerplate들은 다음과 같습니다:

## Dockerfile, docker-compose
본 과제의 Airflow 서비스는 Airflow 공식 이미지를 기반으로한 Dockerfile과, 각 컴포넌트들을 구동시키기 위한 docker-compose로 이루어져 있습니다.
과제의 평가는 제출하신 코드베이스를 docker-compose로 구동하여 진행되기 때문에, VSCode의 dev containers나 제공해드린 docker-compose, Makefile 기반 외의 Python 가상환경 기반의 개발은 지원하지 않습니다.

## 분석계 DB
과제의 진행을 위해 postgres 기반의 가상의 분석계 DB가 구성되어있습니다. 아래의 Makefile이나 docker-compose 커맨드로 별개로 구동시킬 수 있으며, 각종 데이터베이스 접속 툴로도 접근 localhost의 5433 port로 접근하여 내부의 데이터를 확인 할 수 있습니다.

## Makefile
`make build && make up` CMD를 통해서 현재의 로컬 코드베이스의 상태를 기준으로 Docker Image를 빌드하고 해당 이미지를 기준으로 로컬 Airflow 환경을 구동합니다.

`make reset-data` CMD를 통해서 postgres_airflow와 postgres_analytics의 데이터를 초기 상태로 복구 할 수 있습니다.

## Airflow 관련
접속시 id와 pw는 모두 admin입니다.  
초기화 상태의 Airflow는 모든 DAG가 `disabled`되어있는 상태이며, 각 DAG가 어떻게 동작하는지, 에러가 발생하지 않는지 등을 확인하기 위해 `enable`후에 테스트를 해보시기를 추천드립니다.
