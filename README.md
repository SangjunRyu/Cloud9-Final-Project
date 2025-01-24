# Cloud9 Final Project: 서울시 소방 골든타임 데이터 분석 대시보드 구축


## 프로젝트 개요
서울시 소방서의 골든타임 준수를 위해 데이터 기반의 분석 및 시각화 대시보드를 구축하였습니다. 본 프로젝트는 배치 및 실시간 데이터를 활용하여 소방 출동 상황을 분석하고, 골든타임 준수 현황 및 실시간 정보를 제공하는 것을 목표로 합니다.


### 주요 목표
- 서울시 소방 구조출동 데이터를 분석하여 골든 타임(7분) 달성 현황과 실시간 상황 파악
- 서울시 소방서(25개소) 상황실 지원
- 데이터 기반의 정책 결정 지원 및 소방 공무원 근무 여건 개선

---

## 데이터 아키텍처

### Conceptual 아키텍처
![전체 conceptaul 아키텍처](docs/img/conceptual%20architecture.png)


### 배치 파이프라인
- **데이터 수집**: 2017-2023 서울시 구조 출동 데이터
- **ETL**: 결측값 처리, 데이터 타입 변환, 파티셔닝 및 Parquet 저장
- **분석 대상**:
  - 골든타임 준수 여부
  - 출동 원인 분석

![배치 파이프라인](docs/img/batch%20pipeline%20details.png)

### 실시간 파이프라인
- **데이터 수집**:
  - 실시간 돌발 사고 (1분 단위 API 호출)
  - 실시간 교통량 데이터 (1시간 단위 API 호출)
- **변환 및 적재**:
  - GRS80 좌표계 변환
  - 데이터 집계 및 파티셔닝
- **알림 기능**:
  - SNS 이메일 알림 (특정 조건 충족 시)

![실시간 파이프라인](docs/img/realtime%20pipeline%20details.png)

---

## 데이터 시각화 (BI)
**QuickSight**를 활용하여 다음과 같은 대시보드를 제작하였습니다:
1. 골든타임 분석 대시보드 (KPI 차트, 맵 차트, 상자 그림 등)
2. 배치 데이터 분석 대시보드 (워드클라우드, 트리 맵 차트, 막대 차트)

### 주요 분석 결과
- 서초구 포함 17곳의 출동 데이터 중 25% 이상이 골든타임 초과
- 서초구의 소방 공무원 1인당 담당 면적이 자치구 중 가장 높음
- 인력 충원 및 소방서 증설을 통해 골든타임 개선 필요

![SNS 이메일](docs/img/sns%20email.png)


---

## 기대 효과
- 배치 데이터 기반으로 자치구별 소방 공무원 및 소방서 배치 개선
- 실시간 도로 상황 파악을 통한 소방차 신속 출동 지원
- 합리적인 데이터 분석을 통해 정책 결정 지원

---

## 한계점 및 개선 방향
### 한계점
- API 호출 제한으로 인해 데이터 정확도의 한계
- 소방 데이터 API 부재로 수집 과정에서 추가적인 수작업 발생

### 개선 방향
- S3 저장 방식을 RDS로 전환하여 데이터 관리 효율화
- 배치 데이터에 도로 상황을 반영한 복합적 분석 필요

---

## 데이터 소스
- [소방안전 출동 데이터](https://www.bigdata-119.kr/goods/goodsInfo?goods_id=202409000055)
- [서울시 소방 공무원 통계](http://data.seoul.go.kr/dataList/299/S/2/datasetView.do)
- [서울시 지역구별 인구 통계](http://data.seoul.go.kr/dataList/10790/S/2/datasetView.do)
- [서울시 열린데이터광장](https://topis.seoul.go.kr/refRoom/openRefRoom_4.do)

---

## 프로젝트 일정
- **기간**: 2024.12.20 ~ 2025.01.24 (36일)
- **팀 구성**: 유상준 (팀장), 박명근 (부팀장), 정창규

---

## 팀 Jira 링크
- [Cloud9 Jira 보드](https://cloud9petclinic.atlassian.net/jira/software/projects/CF/boards/3)

---

## 발표 자료 경로
- 최종 발표 자료: `docs/presentation/최종 발표-Cloud9(유상준, 박명근, 정창규).pptx`

---

## 이미지 출처
- 배치 파이프라인: `batch pipeline details.png`
- 실시간 파이프라인: `realtime pipeline details.png`
- 대시보드 구조: `conceptual architecture.png`
- SNS 이메일 알림: `sns email.png`

---

### 문의
팀 Cloud9 | 서울시 소방 골든타임 데이터 분석 대시보드 프로젝트
