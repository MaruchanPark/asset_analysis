"""
아파트 매매 실거래가 데이터를 API에서 다운로드하여 xlsx 파일로 저장하는 스크립트
각 지역별 대표 아파트들의 2006년 1월부터 2026년 1월까지 데이터를 수집
"""
import os
import re
import sys
import requests
from urllib.parse import quote
from dotenv import load_dotenv
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import yfinance as yf

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


def parse_target_conditions(file_path: str = "target_condition.txt"):
    """
    target_condition.txt 파일을 파싱하여 지역별 조건을 추출
    
    Returns:
        list: [{"region": "대치동 대표", "lawd_cd": "11680", "conditions": {...}}, ...]
    """
    # 지역명과 지역코드 매핑
    region_mapping = {
        "대치동 대표": {"lawd_cd": "11680", "region_name": "대치동"},  # 강남구
        "압구정 대표": {"lawd_cd": "11680", "region_name": "압구정"},  # 강남구
        "마포구 대표": {"lawd_cd": "11440", "region_name": "마포구"},  # 마포구
        "경기도 대표": {"lawd_cd": "41135", "region_name": "경기도"},  # 성남시 분당구
    }
    
    conditions = []
    
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()
    
    # 각 지역별로 파싱
    sections = re.split(r'\n\n+', content.strip())
    
    for section in sections:
        lines = section.strip().split('\n')
        if not lines:
            continue
        
        region_key = lines[0].replace(':', '').strip()
        
        if region_key not in region_mapping:
            print(f"경고: 알 수 없는 지역 '{region_key}'를 건너뜁니다.")
            continue
        
        # 조건 파싱
        condition_line = None
        for line in lines[1:]:
            if '조건:' in line:
                condition_line = line.split('조건:')[1].strip()
                break
        
        if not condition_line:
            continue
        
        # 조건 파싱: aptNm과 umdNm 조건 추출
        apt_nm_condition = None
        umd_nm_condition = None
        
        # aptNm 조건 파싱
        if 'aptNm ==' in condition_line:
            match = re.search(r'aptNm == "([^"]+)"', condition_line)
            if match:
                apt_nm_condition = {"type": "exact", "value": match.group(1)}
        elif 'aptNm에' in condition_line and '포함' in condition_line:
            match = re.search(r'aptNm에 "([^"]+)" 포함', condition_line)
            if match:
                apt_nm_condition = {"type": "contains", "value": match.group(1)}
        
        # umdNm 조건 파싱
        if 'umdNm ==' in condition_line:
            match = re.search(r'umdNm == "([^"]+)"', condition_line)
            if match:
                umd_nm_condition = {"type": "exact", "value": match.group(1)}
        
        conditions.append({
            "region": region_key,
            "region_name": region_mapping[region_key]["region_name"],
            "lawd_cd": region_mapping[region_key]["lawd_cd"],
            "apt_nm_condition": apt_nm_condition,
            "umd_nm_condition": umd_nm_condition
        })
    
    return conditions


def filter_data_by_condition(items: list, apt_nm_condition: dict, umd_nm_condition: dict):
    """
    조건에 맞는 데이터만 필터링
    
    Args:
        items: API에서 받은 데이터 아이템 리스트
        apt_nm_condition: aptNm 필터 조건
        umd_nm_condition: umdNm 필터 조건
    
    Returns:
        list: 필터링된 아이템 리스트
    """
    filtered = []
    
    for item in items:
        # API 필드명 확인: aptNm, umdNm 사용 (한글 필드명도 지원)
        apt_nm = item.get("aptNm", "") or item.get("아파트", "")
        umd_nm = item.get("umdNm", "") or item.get("법정동", "")
        
        # aptNm 조건 확인
        apt_match = True
        if apt_nm_condition:
            if apt_nm_condition["type"] == "exact":
                apt_match = apt_nm == apt_nm_condition["value"]
            elif apt_nm_condition["type"] == "contains":
                apt_match = apt_nm_condition["value"] in apt_nm
        
        # umdNm 조건 확인
        umd_match = True
        if umd_nm_condition:
            if umd_nm_condition["type"] == "exact":
                umd_match = umd_nm == umd_nm_condition["value"]
        
        if apt_match and umd_match:
            filtered.append(item)
    
    return filtered


def generate_month_range(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    시작년월부터 종료년월까지의 모든 월을 생성
    
    Returns:
        list: ["200601", "200602", ...] 형식의 리스트
    """
    months = []
    year = start_year
    month = start_month
    
    while year < end_year or (year == end_year and month <= end_month):
        months.append(f"{year:04d}{month:02d}")
        
        month += 1
        if month > 12:
            month = 1
            year += 1
    
    return months


def save_to_xlsx(data: dict, output_file: str = None):
    """
    데이터를 xlsx 파일로 저장
    
    Args:
        data: get_apt_trade_data()에서 반환된 데이터
        output_file: 출력 파일명 (없으면 자동 생성)
    """
    if not data["items"]:
        return None
    
    # DataFrame 생성
    df = pd.DataFrame(data["items"])
    
    # 파일명 자동 생성
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"apt_trade_data_{timestamp}.xlsx"
    
    # 디렉토리 생성
    os.makedirs(os.path.dirname(output_file) if os.path.dirname(output_file) else ".", exist_ok=True)
    
    # xlsx 파일로 저장
    df.to_excel(output_file, index=False, engine="openpyxl")
    return output_file


def download_financial_data(ticker: str, asset_name: str, start_year: int, start_month: int, end_year: int, end_month: int):
    """
    금융 자산 데이터를 yfinance를 통해 다운로드하여 저장 (범용 함수)
    
    Args:
        ticker: yfinance 티커 심볼 (예: "QQQ", "BTC-USD", "IAU")
        asset_name: 자산 이름 (디렉토리명 및 파일명에 사용)
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    print(f"\n{'='*60}")
    print(f"{asset_name} 데이터 수집 중...")
    print(f"{'='*60}")
    
    # 시작일과 종료일 설정
    start_date = datetime(start_year, start_month, 1)
    # 종료일은 해당 월의 마지막 날로 설정
    if end_month == 12:
        end_date = datetime(end_year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(end_year, end_month + 1, 1) - timedelta(days=1)
    
    print(f"기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
    
    try:
        print(f"티커: {ticker} 다운로드 중...", end=" ", flush=True)
        
        data = yf.download(ticker, start=start_date, end=end_date + timedelta(days=1), progress=False)
        
        if data.empty:
            print("[ERROR] 데이터를 가져올 수 없습니다.")
            return None
        
        print(f"[OK] {len(data)}건의 데이터 수집 완료")
        
        # 월별 종가 데이터 추출 (각 월의 마지막 거래일 종가 사용)
        data['Year'] = data.index.year
        data['Month'] = data.index.month
        
        # 각 년월별로 마지막 거래일의 종가 선택
        monthly_data = data.groupby(['Year', 'Month']).last().reset_index()
        
        # 거래년월 컬럼 추가 (YYYYMM 형식)
        monthly_data['거래년월'] = monthly_data['Year'].astype(str) + monthly_data['Month'].astype(str).str.zfill(2)
        
        # 필요한 컬럼만 선택 (values를 사용하여 1차원 배열로 변환)
        result_df = pd.DataFrame({
            '거래년월': monthly_data['거래년월'].values,
            '거래일자': pd.to_datetime(monthly_data['거래년월'].values, format='%Y%m', errors='coerce'),
            '종가': monthly_data['Close'].values.flatten() if monthly_data['Close'].ndim > 1 else monthly_data['Close'].values,
            '시가': monthly_data['Open'].values.flatten() if monthly_data['Open'].ndim > 1 else monthly_data['Open'].values,
            '고가': monthly_data['High'].values.flatten() if monthly_data['High'].ndim > 1 else monthly_data['High'].values,
            '저가': monthly_data['Low'].values.flatten() if monthly_data['Low'].ndim > 1 else monthly_data['Low'].values,
            '거래량': monthly_data['Volume'].values.flatten() if monthly_data['Volume'].ndim > 1 else monthly_data['Volume'].values
        })
        
        # 시작일 이후 데이터만 필터링
        start_ym = f"{start_year:04d}{start_month:02d}"
        result_df = result_df[result_df['거래년월'] >= start_ym]
        
        # 종료일 이전 데이터만 필터링
        end_ym = f"{end_year:04d}{end_month:02d}"
        result_df = result_df[result_df['거래년월'] <= end_ym]
        
        # 저장 디렉토리 생성
        region_dir = f"data/{asset_name}"
        os.makedirs(region_dir, exist_ok=True)
        
        # 파일 저장
        output_file = f"{region_dir}/{asset_name}_{start_year:04d}{start_month:02d}_{end_year:04d}{end_month:02d}.xlsx"
        result_df.to_excel(output_file, index=False, engine="openpyxl")
        
        print(f"[OK] {asset_name} 데이터 저장 완료: {output_file}")
        print(f"  총 {len(result_df)}건의 데이터가 저장되었습니다.")
        
        return output_file
        
    except Exception as e:
        print(f"[ERROR] {asset_name} 데이터 수집 오류: {e}")
        import traceback
        traceback.print_exc()
        return None


def download_nasdaq100_data(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    나스닥 100 데이터를 yfinance를 통해 다운로드하여 저장
    
    Args:
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    # QQQ는 나스닥 100을 추적하는 ETF로 더 안정적인 데이터 제공
    return download_financial_data("QQQ", "나스닥100", start_year, start_month, end_year, end_month)


def download_bitcoin_data(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    비트코인 데이터를 yfinance를 통해 다운로드하여 저장
    
    Args:
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    return download_financial_data("BTC-USD", "비트코인", start_year, start_month, end_year, end_month)


def download_iau_data(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    IAU (금 ETF) 데이터를 yfinance를 통해 다운로드하여 저장
    
    Args:
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    return download_financial_data("IAU", "IAU", start_year, start_month, end_year, end_month)


def download_kospi100_data(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    코스피100 데이터를 yfinance를 통해 다운로드하여 저장
    
    Args:
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    # 코스피100은 ^KS100 또는 다른 티커 사용 가능
    # yfinance에서는 ^KS100이 작동하지 않을 수 있으므로 KODEX KOSPI100 ETF (069500.KS) 사용
    return download_financial_data("069500.KS", "코스피100", start_year, start_month, end_year, end_month)


def download_cqqq_data(start_year: int, start_month: int, end_year: int, end_month: int):
    """
    CQQQ (중국 기술주 ETF) 데이터를 yfinance를 통해 다운로드하여 저장
    
    Args:
        start_year: 시작 연도
        start_month: 시작 월
        end_year: 종료 연도
        end_month: 종료 월
    
    Returns:
        str: 저장된 파일 경로
    """
    return download_financial_data("CQQQ", "CQQQ", start_year, start_month, end_year, end_month)


def main():
    """메인 함수: 각 지역별 대표 아파트들의 2006년 2월부터 2026년 1월까지 데이터 수집"""
    
    # 명령줄 인자 확인
    financial_only = "--financial-only" in sys.argv or "-f" in sys.argv
    nasdaq_only = "--nasdaq-only" in sys.argv or "-n" in sys.argv
    
    # 월 범위 생성 (2006-02 ~ 2026-01)
    start_year, start_month = 2006, 2
    end_year, end_month = 2026, 1
    
    # 금융 자산만 수집하는 경우
    if financial_only:
        print("=" * 60)
        print("금융 자산 데이터만 수집합니다.")
        print("=" * 60)
        
        results = []
        
        # 모든 금융 자산 데이터 수집
        results.append(("나스닥100", download_nasdaq100_data(start_year, start_month, end_year, end_month)))
        results.append(("비트코인", download_bitcoin_data(start_year, start_month, end_year, end_month)))
        results.append(("IAU", download_iau_data(start_year, start_month, end_year, end_month)))
        results.append(("코스피100", download_kospi100_data(start_year, start_month, end_year, end_month)))
        results.append(("CQQQ", download_cqqq_data(start_year, start_month, end_year, end_month)))
        
        print(f"\n{'='*60}")
        print("금융 자산 데이터 수집 결과:")
        print("-" * 60)
        success_count = 0
        for name, result in results:
            status = "성공" if result else "실패"
            print(f"  {name}: {status}")
            if result:
                success_count += 1
        print(f"\n총 {success_count}/{len(results)}개 자산 수집 완료")
        print(f"{'='*60}")
        return 0 if success_count > 0 else 1
    
    # 나스닥만 수집하는 경우 (하위 호환성)
    if nasdaq_only:
        print("=" * 60)
        print("나스닥 100 데이터만 수집합니다.")
        print("=" * 60)
        
        # 나스닥 100 데이터 수집
        result = download_nasdaq100_data(start_year, start_month, end_year, end_month)
        
        print(f"\n{'='*60}")
        if result:
            print("나스닥 100 데이터 수집이 완료되었습니다!")
        else:
            print("나스닥 100 데이터 수집 중 오류가 발생했습니다.")
        print(f"{'='*60}")
        return 0 if result else 1
    
    # 일반 모드: 부동산 데이터 + 나스닥 데이터 모두 수집
    # target_condition.txt 파싱
    print("target_condition.txt 파일을 파싱하는 중...")
    try:
        conditions = parse_target_conditions("target_condition.txt")
        print(f"총 {len(conditions)}개 지역의 조건을 로드했습니다.")
    except Exception as e:
        print(f"조건 파일 파싱 오류: {e}")
        return 1
    
    # 월 범위 생성 (2006-02 ~ 2026-01)
    start_year, start_month = 2006, 2
    end_year, end_month = 2026, 1
    months = generate_month_range(start_year, start_month, end_year, end_month)
    print(f"총 {len(months)}개월의 데이터를 수집합니다. ({start_year:04d}{start_month:02d} ~ {end_year:04d}{end_month:02d})")
    
    # 각 지역별로 데이터 수집
    for condition in conditions:
        region_name = condition["region_name"]
        lawd_cd = condition["lawd_cd"]
        apt_nm_condition = condition["apt_nm_condition"]
        umd_nm_condition = condition["umd_nm_condition"]
        
        print(f"\n{'='*60}")
        print(f"지역: {condition['region']} ({region_name})")
        print(f"지역코드: {lawd_cd}")
        print(f"조건: aptNm={apt_nm_condition}, umdNm={umd_nm_condition}")
        print(f"{'='*60}")
        
        # 지역별 디렉토리 생성
        region_dir = f"data/{region_name}"
        os.makedirs(region_dir, exist_ok=True)
        
        # 각 월별로 데이터 수집
        all_filtered_data = []
        
        for month in months:
            print(f"  [{month}] 데이터 수집 중...", end=" ", flush=True)
            
            try:
                # 데이터 조회
                data = get_apt_trade_data(lawd_cd=lawd_cd, deal_ymd=month)
                
                # 조건에 맞는 데이터 필터링
                filtered_items = filter_data_by_condition(
                    data["items"],
                    apt_nm_condition,
                    umd_nm_condition
                )
                
                if filtered_items:
                    # 거래년월 정보 추가
                    for item in filtered_items:
                        item["거래년월"] = month
                    all_filtered_data.extend(filtered_items)
                    print(f"[OK] {len(filtered_items)}건 발견")
                else:
                    print("[OK] 0건")
                
            except Exception as e:
                print(f"[ERROR] 오류: {e}")
                continue
        
        # 필터링된 데이터를 하나의 파일로 저장
        if all_filtered_data:
            output_file = f"{region_dir}/{region_name}_{start_year:04d}{start_month:02d}_{end_year:04d}{end_month:02d}.xlsx"
            result = save_to_xlsx({"items": all_filtered_data}, output_file)
            if result:
                print(f"\n[OK] {region_name} 데이터 저장 완료: {result}")
                print(f"  총 {len(all_filtered_data)}건의 데이터가 저장되었습니다.")
        else:
            print(f"\n[WARNING] {region_name}: 조건에 맞는 데이터가 없습니다.")
    
    # 모든 금융 자산 데이터 수집
    print(f"\n{'='*60}")
    print("금융 자산 데이터 수집 중...")
    print(f"{'='*60}")
    
    download_nasdaq100_data(start_year, start_month, end_year, end_month)
    download_bitcoin_data(start_year, start_month, end_year, end_month)
    download_iau_data(start_year, start_month, end_year, end_month)
    download_kospi100_data(start_year, start_month, end_year, end_month)
    download_cqqq_data(start_year, start_month, end_year, end_month)
    
    print(f"\n{'='*60}")
    print("모든 데이터 수집이 완료되었습니다!")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    exit(main())
