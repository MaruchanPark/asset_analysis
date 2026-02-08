"""
아파트 매매 실거래가 데이터를 API에서 다운로드하여 xlsx 파일로 저장하는 스크립트
"""
import os
import requests
from urllib.parse import quote
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime

# .env 파일에서 환경변수 로드
load_dotenv()

def get_apt_trade_data(lawd_cd: str, deal_ymd: str, api_key: str = None):
    """
    아파트 매매 실거래가 데이터를 API에서 조회
    
    Args:
        lawd_cd: 지역코드 (5자리 숫자, 예: "11110")
        deal_ymd: 거래년월 (6자리 숫자, YYYYMM 형식, 예: "201001")
        api_key: API 키 (없으면 .env의 API_KEY 사용)
    
    Returns:
        dict: API 응답 데이터
    """
    if api_key is None:
        api_key = os.getenv("API_KEY")
    
    if not api_key:
        raise ValueError("API_KEY가 설정되지 않았습니다. .env 파일에 API_KEY를 설정하세요.")
    
    # API 엔드포인트
    base_url = "https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade"
    
    # 파라미터 설정
    params = {
        "LAWD_CD": lawd_cd,
        "DEAL_YMD": deal_ymd,
        "serviceKey": api_key,
        "pageNo": 1,
        "numOfRows": 1000  # 한 번에 최대 1000개까지 조회 가능
    }
    
    # API 요청
    response = requests.get(base_url, params=params)
    response.raise_for_status()
    
    # XML 파싱
    root = ET.fromstring(response.content)
    
    # 결과 코드 확인
    result_code = root.find(".//resultCode")
    result_msg = root.find(".//resultMsg")
    
    if result_code is not None and result_code.text != "000":
        error_msg = result_msg.text if result_msg is not None else "알 수 없는 오류"
        raise Exception(f"API 오류: {error_msg} (코드: {result_code.text})")
    
    # 전체 건수 확인
    total_count_elem = root.find(".//totalCount")
    total_count = int(total_count_elem.text) if total_count_elem is not None else 0
    
    # 데이터 추출
    items = []
    for item in root.findall(".//item"):
        item_data = {}
        for child in item:
            item_data[child.tag] = child.text if child.text else ""
        items.append(item_data)
    
    # 페이지네이션 처리 (전체 데이터가 1000개를 초과하는 경우)
    all_items = items.copy()
    page_no = 1
    
    while len(all_items) < total_count:
        page_no += 1
        params["pageNo"] = page_no
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        root = ET.fromstring(response.content)
        items = []
        for item in root.findall(".//item"):
            item_data = {}
            for child in item:
                item_data[child.tag] = child.text if child.text else ""
            items.append(item_data)
        
        if not items:  # 더 이상 데이터가 없으면 종료
            break
        
        all_items.extend(items)
    
    return {
        "total_count": total_count,
        "items": all_items
    }


def save_to_xlsx(data: dict, output_file: str = None):
    """
    데이터를 xlsx 파일로 저장
    
    Args:
        data: get_apt_trade_data()에서 반환된 데이터
        output_file: 출력 파일명 (없으면 자동 생성)
    """
    if not data["items"]:
        print("저장할 데이터가 없습니다.")
        return
    
    # DataFrame 생성
    df = pd.DataFrame(data["items"])
    
    # 파일명 자동 생성
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"apt_trade_data_{timestamp}.xlsx"
    
    # xlsx 파일로 저장
    df.to_excel(output_file, index=False, engine="openpyxl")
    print(f"데이터가 {output_file}에 저장되었습니다.")
    print(f"총 {len(df)}건의 데이터가 저장되었습니다.")


def main():
    """메인 함수"""
    # 예제: 서울 종로구, 2010년 1월 데이터
    # lawd_cd = "11680"  # 서울 강남구
    # lawd_cd = "11440"  # 서울 마포구
    lawd_cd = "41135"  # 성남시 분당구
    deal_ymd = "200601"
    
    print(f"지역코드: {lawd_cd}, 거래년월: {deal_ymd}")
    print("데이터를 다운로드하는 중...")
    
    try:
        # 데이터 조회
        data = get_apt_trade_data(lawd_cd=lawd_cd, deal_ymd=deal_ymd)
        
        print(f"총 {data['total_count']}건의 데이터를 조회했습니다.")
        
        # xlsx 파일로 저장
        output_file = f"apt_trade_{lawd_cd}_{deal_ymd}.xlsx"
        save_to_xlsx(data, output_file)
        
    except Exception as e:
        print(f"오류 발생: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
