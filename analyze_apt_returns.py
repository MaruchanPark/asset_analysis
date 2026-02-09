"""
지역별 월별 수익률 분석 및 시각화 스크립트
1평당 가격으로 정규화하여 비교 (각 지역의 모든 아파트를 하나의 변수로 취급)
"""
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from datetime import datetime
import numpy as np

# 한글 폰트 설정
plt.rcParams['font.family'] = 'Malgun Gothic'  # Windows
plt.rcParams['axes.unicode_minus'] = False

# 1평 = 3.3㎡
PYEONG_TO_SQM = 3.3


def load_all_data(data_dir: str = "data"):
    """
    모든 지역의 Excel 파일을 로드하여 하나의 DataFrame으로 합침
    금융 자산 데이터는 제외 (별도 처리)
    
    Args:
        data_dir: 데이터 디렉토리 경로
    
    Returns:
        pd.DataFrame: 모든 데이터를 합친 DataFrame
    """
    all_data = []
    
    # 제외할 금융 자산 디렉토리 목록
    excluded_financial_assets = ["나스닥100", "비트코인", "IAU", "코스피100", "CQQQ"]
    
    # data 디렉토리 내의 모든 하위 디렉토리 탐색
    for region_dir in os.listdir(data_dir):
        region_path = os.path.join(data_dir, region_dir)
        
        if not os.path.isdir(region_path):
            continue
        
        # 금융 자산은 별도 처리하므로 건너뜀
        if region_dir in excluded_financial_assets:
            continue
        
        # 각 지역 디렉토리 내의 Excel 파일 찾기
        for file in os.listdir(region_path):
            if file.endswith('.xlsx'):
                file_path = os.path.join(region_path, file)
                print(f"로딩 중: {file_path}")
                
                try:
                    df = pd.read_excel(file_path)
                    # 지역 정보 추가
                    df['지역'] = region_dir
                    all_data.append(df)
                except Exception as e:
                    print(f"  오류: {file_path} 로딩 실패 - {e}")
    
    if not all_data:
        raise ValueError("로드할 데이터가 없습니다.")
    
    # 모든 데이터 합치기
    combined_df = pd.concat(all_data, ignore_index=True)
    print(f"\n총 {len(combined_df)}건의 데이터를 로드했습니다.")
    
    return combined_df


def load_financial_data(data_dir: str, asset_name: str, display_name: str = None):
    """
    금융 자산 데이터를 로드하여 수익률 계산용 DataFrame으로 변환 (범용 함수)
    
    Args:
        data_dir: 데이터 디렉토리 경로
        asset_name: 자산 디렉토리명 (예: "나스닥100", "비트코인")
        display_name: 표시할 이름 (None이면 asset_name 사용)
    
    Returns:
        pd.DataFrame: 수익률 데이터 (지역=display_name, 거래년월, 수익률 컬럼 포함)
    """
    if display_name is None:
        display_name = asset_name
    
    asset_dir = os.path.join(data_dir, asset_name)
    
    if not os.path.isdir(asset_dir):
        print(f"경고: {asset_name} 데이터 디렉토리를 찾을 수 없습니다.")
        return None
    
    # Excel 파일 찾기
    asset_files = [f for f in os.listdir(asset_dir) if f.endswith('.xlsx') and not f.startswith('~$')]
    
    if not asset_files:
        print(f"경고: {asset_name} 데이터 파일을 찾을 수 없습니다.")
        return None
    
    # 가장 최근 파일 사용 (또는 첫 번째 파일)
    asset_file = os.path.join(asset_dir, asset_files[0])
    print(f"{display_name} 데이터 로딩 중: {asset_file}")
    
    try:
        df = pd.read_excel(asset_file)
        
        # 거래년월을 Period 타입으로 변환
        df['거래년월'] = pd.to_datetime(df['거래년월'].astype(str), format='%Y%m', errors='coerce').dt.to_period('M')
        
        # 종가 데이터 확인
        if '종가' not in df.columns:
            print(f"경고: {asset_name} 데이터에 '종가' 컬럼이 없습니다.")
            return None
        
        # 데이터 정렬
        df = df.sort_values('거래년월')
        df = df.dropna(subset=['거래년월', '종가'])
        
        # 첫 번째 월 가격을 기준으로 누적 수익률 계산
        if len(df) > 0 and df['종가'].iloc[0] > 0:
            base_price = df['종가'].iloc[0]
            df['수익률'] = ((df['종가'] - base_price) / base_price) * 100
        else:
            df['수익률'] = 0
        
        # 지역 컬럼 추가
        df['지역'] = display_name
        
        # 필요한 컬럼만 선택
        result_df = df[['지역', '거래년월', '수익률', '종가']].copy()
        result_df.rename(columns={'종가': '평당가격'}, inplace=True)  # 일관성을 위해 컬럼명 변경
        
        print(f"{display_name} 데이터 로드 완료: {len(result_df)}건")
        
        return result_df
        
    except Exception as e:
        print(f"{asset_name} 데이터 로딩 오류: {e}")
        import traceback
        traceback.print_exc()
        return None


def load_nasdaq100_data(data_dir: str = "data"):
    """
    나스닥 100 데이터를 로드하여 수익률 계산용 DataFrame으로 변환
    
    Args:
        data_dir: 데이터 디렉토리 경로
    
    Returns:
        pd.DataFrame: 나스닥 100 수익률 데이터 (지역='나스닥100', 거래년월, 수익률 컬럼 포함)
    """
    return load_financial_data(data_dir, "나스닥100", "나스닥100")


def preprocess_data(df: pd.DataFrame):
    """
    데이터 전처리: 필요한 컬럼 정리 및 타입 변환
    
    Args:
        df: 원본 DataFrame
    
    Returns:
        pd.DataFrame: 전처리된 DataFrame
    """
    # 필요한 컬럼 확인 및 선택
    required_cols = ['aptNm', 'dealAmount', 'excluUseAr', 'dealYear', 'dealMonth', 'dealDay']
    
    # 한글 컬럼명도 확인
    col_mapping = {
        '아파트': 'aptNm',
        '거래금액': 'dealAmount',
        '전용면적': 'excluUseAr',
        '년': 'dealYear',
        '월': 'dealMonth',
        '일': 'dealDay',
    }
    
    # 컬럼명 매핑
    df = df.rename(columns=col_mapping)
    
    # 필요한 컬럼만 선택
    available_cols = [col for col in required_cols if col in df.columns]
    if '지역' in df.columns:
        available_cols.append('지역')
    
    df = df[available_cols].copy()
    
    # 데이터 타입 변환
    # dealAmount: 문자열에서 숫자로 변환 (예: "28,000" -> 28000)
    # 주의: dealAmount는 만원 단위로 저장되어 있음
    if 'dealAmount' in df.columns:
        df['dealAmount'] = df['dealAmount'].astype(str).str.replace(',', '').astype(float)
    
    # excluUseAr: 전용면적을 float로 변환
    if 'excluUseAr' in df.columns:
        df['excluUseAr'] = pd.to_numeric(df['excluUseAr'], errors='coerce')
    
    # 거래일자 생성
    if all(col in df.columns for col in ['dealYear', 'dealMonth', 'dealDay']):
        df['거래일자'] = pd.to_datetime(
            df['dealYear'].astype(str) + '-' + 
            df['dealMonth'].astype(str).str.zfill(2) + '-' + 
            df['dealDay'].astype(str).str.zfill(2),
            errors='coerce'
        )
        df['거래년월'] = df['거래일자'].dt.to_period('M')
    elif '거래년월' in df.columns:
        # 이미 거래년월이 있는 경우
        df['거래년월'] = pd.to_datetime(df['거래년월'].astype(str), format='%Y%m', errors='coerce').dt.to_period('M')
    
    # 필수 데이터가 없는 행 제거
    df = df.dropna(subset=['aptNm', 'dealAmount', 'excluUseAr', '거래년월'])
    
    # 전용면적이 0이거나 음수인 경우 제거
    df = df[df['excluUseAr'] > 0]
    
    # 1평당 가격 계산 (1평 = 3.3㎡)
    # dealAmount는 만원 단위이므로 평당가격도 만원/평 단위로 계산됨
    df['평수'] = df['excluUseAr'] / PYEONG_TO_SQM
    df['평당가격'] = df['dealAmount'] / df['평수']  # 단위: 만원/평
    
    return df


def calculate_monthly_returns(df: pd.DataFrame):
    """
    지역별 월별 수익률 계산
    각 지역의 모든 아파트를 하나의 변수로 취급하여 1평당 평균 가격으로 계산
    
    Args:
        df: 전처리된 DataFrame
    
    Returns:
        pd.DataFrame: 지역별 월별 수익률 데이터
    """
    # 지역별, 월별로 그룹화하여 평균 가격 계산 (모든 아파트를 합쳐서)
    monthly_avg = df.groupby(['지역', '거래년월']).agg({
        '평당가격': 'mean'  # 각 지역의 모든 아파트 거래의 평균 1평당 가격
    }).reset_index()
    
    # 지역별로 정렬
    monthly_avg = monthly_avg.sort_values(['지역', '거래년월'])
    
    # 수익률 계산: 첫 번째 월 기준 누적 수익률 (모든 지역이 같은 시작점 0%에서 시작)
    returns_data = []
    
    for region in monthly_avg['지역'].unique():
        region_data = monthly_avg[monthly_avg['지역'] == region].copy()
        region_data = region_data.sort_values('거래년월')
        
        # 첫 번째 월 가격을 기준으로 누적 수익률 계산
        if len(region_data) > 0 and region_data['평당가격'].iloc[0] > 0:
            base_price = region_data['평당가격'].iloc[0]
            region_data['수익률'] = ((region_data['평당가격'] - base_price) / base_price) * 100
        else:
            region_data['수익률'] = 0
        
        returns_data.append(region_data)
    
    returns_df = pd.concat(returns_data, ignore_index=True)
    
    return returns_df


def plot_returns_comparison(returns_df: pd.DataFrame, output_file: str = "apt_returns_comparison.png"):
    """
    지역별 월별 수익률을 그래프로 시각화
    
    Args:
        returns_df: 수익률 데이터 DataFrame
        output_file: 출력 파일명
    """
    # 지역별로 그래프 생성
    region_list = sorted(returns_df['지역'].unique())
    
    # 그래프 크기 설정
    fig, axes = plt.subplots(len(region_list), 1, figsize=(14, 4 * len(region_list)))
    
    if len(region_list) == 1:
        axes = [axes]
    
    for idx, region in enumerate(region_list):
        region_data = returns_df[returns_df['지역'] == region].copy()
        region_data = region_data.sort_values('거래년월')
        
        # 거래년월을 문자열로 변환하여 x축에 사용
        x_labels = [str(ym) for ym in region_data['거래년월']]
        x_positions = range(len(x_labels))
        
        # 수익률 그래프
        ax = axes[idx]
        ax.plot(x_positions, region_data['수익률'], marker='o', linewidth=1.5, markersize=3, label='월별 수익률 (%)', alpha=0.7)
        ax.axhline(y=0, color='gray', linestyle='--', linewidth=1, alpha=0.5)
        ax.set_title(f'{region} - 월별 수익률 (1평당 기준)', fontsize=14, fontweight='bold')
        ax.set_xlabel('거래년월', fontsize=12)
        ax.set_ylabel('수익률 (%)', fontsize=12)
        ax.set_xticks(x_positions[::max(1, len(x_positions)//10)])  # 너무 많으면 일부만 표시
        ax.set_xticklabels([x_labels[i] for i in x_positions[::max(1, len(x_positions)//10)]], rotation=45, ha='right')
        ax.grid(True, alpha=0.3)
        ax.legend()
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n그래프 저장 완료: {output_file}")
    plt.close()


def plot_combined_comparison(returns_df: pd.DataFrame, financial_data_dict: dict = None, output_file: str = "apt_returns_combined.png", include_bitcoin: bool = True):
    """
    모든 지역의 수익률을 하나의 그래프에 비교 (4개 지역 + 금융 자산들)
    
    Args:
        returns_df: 수익률 데이터 DataFrame
        financial_data_dict: 금융 자산 데이터 딕셔너리 {자산명: DataFrame} (선택사항)
        output_file: 출력 파일명
        include_bitcoin: 비트코인 포함 여부 (기본값: True)
    """
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # 4개 지역만 필터링: 경기도, 대치동, 마포구, 압구정
    target_regions = ['경기도', '대치동', '마포구', '압구정']
    region_list = [r for r in sorted(returns_df['지역'].unique()) if r in target_regions]
    
    # 색상 설정 (4개 지역)
    region_colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']  # 파란색, 주황색, 초록색, 빨간색
    
    # 금융 자산 색상 설정
    financial_colors = {
        '나스닥100': '#9467bd',  # 보라색
        '비트코인': '#f7931a',   # 주황색
        'IAU': '#ffd700',        # 금색
        '코스피100': '#8b0000',  # 진한 빨간색
        'CQQQ': '#00bfff'        # 하늘색
    }
    
    # 금융 자산 마커 및 스타일 설정
    financial_styles = {
        '나스닥100': ('s', '--'),
        '비트코인': ('^', '-.'),
        'IAU': ('D', ':'),
        '코스피100': ('v', '--'),
        'CQQQ': ('p', '-.')
    }
    
    # 2006년 2월부터 데이터만 사용하기 위해 필터링
    start_period = pd.Period('2006-02', freq='M')
    
    # 지역별 데이터 플롯
    for idx, region in enumerate(region_list):
        region_data = returns_df[returns_df['지역'] == region].copy()
        region_data = region_data.sort_values('거래년월')
        
        # 2006년 2월 이후 데이터만 필터링
        region_data = region_data[region_data['거래년월'] >= start_period]
        
        if len(region_data) == 0:
            continue
        
        # 거래년월을 문자열로 변환
        x_labels = [str(ym) for ym in region_data['거래년월']]
        x_positions = range(len(x_labels))
        
        ax.plot(x_positions, region_data['수익률'], marker='o', linewidth=2, 
               markersize=3, label=region, color=region_colors[idx % len(region_colors)], alpha=0.8)
    
    # 금융 자산 데이터 플롯
    if financial_data_dict:
        # x축을 맞추기 위해 첫 번째 지역의 날짜 기준으로 매핑
        if len(region_list) > 0:
            first_region = returns_df[returns_df['지역'] == region_list[0]].sort_values('거래년월')
            first_region = first_region[first_region['거래년월'] >= start_period]
            first_region_labels = [str(ym) for ym in first_region['거래년월']]
            
            for asset_name, asset_df in financial_data_dict.items():
                # 비트코인 제외 옵션 처리
                if not include_bitcoin and asset_name == '비트코인':
                    continue
                
                if asset_df is None or len(asset_df) == 0:
                    continue
                
                asset_data = asset_df.copy()
                asset_data = asset_data.sort_values('거래년월')
                
                # 2006년 2월 이후 데이터만 필터링
                asset_data = asset_data[asset_data['거래년월'] >= start_period]
                
                if len(asset_data) == 0:
                    continue
                
                # 각 월을 첫 번째 지역의 인덱스에 매핑
                asset_x_positions = []
                asset_returns = []
                
                for _, row in asset_data.iterrows():
                    asset_period = str(row['거래년월'])
                    if asset_period in first_region_labels:
                        idx = first_region_labels.index(asset_period)
                        asset_x_positions.append(idx)
                        asset_returns.append(row['수익률'])
                
                if len(asset_x_positions) > 0:
                    color = financial_colors.get(asset_name, '#808080')
                    marker, linestyle = financial_styles.get(asset_name, ('o', '-'))
                    ax.plot(asset_x_positions, asset_returns, marker=marker, linewidth=2, 
                           markersize=3, label=asset_name, color=color, alpha=0.8, linestyle=linestyle)
    
    ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
    
    # 제목 생성
    if financial_data_dict:
        financial_names = [name for name in financial_data_dict.keys() 
                          if include_bitcoin or name != '비트코인']
        if financial_names:
            title = '지역별 월별 수익률 비교 (1평당 기준) + ' + ', '.join(financial_names)
        else:
            title = '지역별 월별 수익률 비교 (1평당 기준)'
    else:
        title = '지역별 월별 수익률 비교 (1평당 기준)'
    ax.set_title(title, fontsize=16, fontweight='bold')
    
    ax.set_xlabel('거래년월', fontsize=12)
    ax.set_ylabel('수익률 (%)', fontsize=12)
    
    # x축 레이블 설정 (첫 번째 지역의 날짜 사용)
    if len(region_list) > 0:
        first_region = returns_df[returns_df['지역'] == region_list[0]].sort_values('거래년월')
        first_region = first_region[first_region['거래년월'] >= start_period]
        x_labels = [str(ym) for ym in first_region['거래년월']]
        x_positions = range(len(x_labels))
        step = max(1, len(x_positions)//15)
        ax.set_xticks(x_positions[::step])
        ax.set_xticklabels([x_labels[i] for i in x_positions[::step]], rotation=45, ha='right')
    
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=10, ncol=2)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"통합 비교 그래프 저장 완료: {output_file}")
    plt.close()


def plot_absolute_price_comparison(returns_df: pd.DataFrame, output_file: str = "apt_absolute_price.png"):
    """
    지역별 월별 절대 가격 비교 그래프 (4개 지역만 표시)
    검증을 위해 실제 1평당 가격을 표시
    
    Args:
        returns_df: 수익률 데이터 DataFrame (평당가격 정보 포함)
        output_file: 출력 파일명
    """
    fig, ax = plt.subplots(figsize=(16, 8))
    
    # 4개 지역만 필터링: 경기도, 대치동, 마포구, 압구정
    target_regions = ['경기도', '대치동', '마포구', '압구정']
    region_list = [r for r in sorted(returns_df['지역'].unique()) if r in target_regions]
    
    # 4개 색상만 사용
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']  # 파란색, 주황색, 초록색, 빨간색
    
    for idx, region in enumerate(region_list):
        region_data = returns_df[returns_df['지역'] == region].copy()
        region_data = region_data.sort_values('거래년월')
        
        # 절대 가격 사용 (단위: 만원/평)
        # dealAmount가 이미 만원 단위이므로 평당가격도 만원/평 단위임
        x_labels = [str(ym) for ym in region_data['거래년월']]
        x_positions = range(len(x_labels))
        
        # 평당가격은 이미 만원/평 단위이므로 그대로 사용
        price_in_manwon = region_data['평당가격']
        
        ax.plot(x_positions, price_in_manwon, marker='o', linewidth=2, 
               markersize=3, label=region, color=colors[idx % len(colors)], alpha=0.8)
    
    ax.set_title('지역별 월별 절대 가격 비교 (1평당 기준)', fontsize=16, fontweight='bold')
    ax.set_xlabel('거래년월', fontsize=12)
    ax.set_ylabel('1평당 가격 (만원)', fontsize=12)
    
    # x축 레이블 설정 (첫 번째 지역의 날짜 사용)
    if len(region_list) > 0:
        first_region = returns_df[returns_df['지역'] == region_list[0]].sort_values('거래년월')
        x_labels = [str(ym) for ym in first_region['거래년월']]
        x_positions = range(len(x_labels))
        step = max(1, len(x_positions)//15)
        ax.set_xticks(x_positions[::step])
        ax.set_xticklabels([x_labels[i] for i in x_positions[::step]], rotation=45, ha='right')
    
    ax.grid(True, alpha=0.3)
    ax.legend(loc='best', fontsize=12)
    
    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"절대 가격 비교 그래프 저장 완료: {output_file}")
    plt.close()


def main():
    """메인 함수"""
    print("=" * 60)
    print("지역별 월별 수익률 분석 시작")
    print("=" * 60)
    
    try:
        # 1. 데이터 로드
        print("\n[1단계] 데이터 로드 중...")
        df = load_all_data("data")
        
        # 2. 데이터 전처리
        print("\n[2단계] 데이터 전처리 중...")
        df_processed = preprocess_data(df)
        print(f"전처리 완료: {len(df_processed)}건의 데이터")
        
        # 3. 수익률 계산
        print("\n[3단계] 월별 수익률 계산 중...")
        returns_df = calculate_monthly_returns(df_processed)
        print(f"수익률 계산 완료: {len(returns_df)}건")
        
        # 금융 자산 데이터 로드
        print("\n[3-1단계] 금융 자산 데이터 로드 중...")
        financial_assets = {
            '나스닥100': load_financial_data("data", "나스닥100", "나스닥100"),
            '비트코인': load_financial_data("data", "비트코인", "비트코인"),
            'IAU': load_financial_data("data", "IAU", "IAU"),
            '코스피100': load_financial_data("data", "코스피100", "코스피100"),
            'CQQQ': load_financial_data("data", "CQQQ", "CQQQ")
        }
        
        # None 값 제거
        financial_assets = {k: v for k, v in financial_assets.items() if v is not None}
        
        # 지역별 통계 출력
        print("\n지역별 수익률 통계:")
        print("-" * 60)
        target_regions = ['경기도', '대치동', '마포구', '압구정']
        for region in sorted(returns_df['지역'].unique()):
            if region in target_regions:
                region_returns = returns_df[returns_df['지역'] == region]['수익률']
                print(f"{region}:")
                print(f"  평균 수익률: {region_returns.mean():.2f}%")
                print(f"  최대 수익률: {region_returns.max():.2f}%")
                print(f"  최소 수익률: {region_returns.min():.2f}%")
                print(f"  표준편차: {region_returns.std():.2f}%")
                print()
        
        # 금융 자산 통계 출력
        if financial_assets:
            print("금융 자산 수익률 통계:")
            print("-" * 60)
            for asset_name, asset_df in financial_assets.items():
                if asset_df is not None and len(asset_df) > 0:
                    asset_returns = asset_df['수익률']
                    print(f"{asset_name}:")
                    print(f"  평균 수익률: {asset_returns.mean():.2f}%")
                    print(f"  최대 수익률: {asset_returns.max():.2f}%")
                    print(f"  최소 수익률: {asset_returns.min():.2f}%")
                    print(f"  표준편차: {asset_returns.std():.2f}%")
                    print()
        
        # 4. 그래프 생성
        print("\n[4단계] 그래프 생성 중...")
        plot_returns_comparison(returns_df, "apt_returns_comparison.png")
        # 비트코인 포함 그래프
        plot_combined_comparison(returns_df, financial_assets, "returns_bitcoin.png", include_bitcoin=True)
        # 비트코인 제외 그래프
        plot_combined_comparison(returns_df, financial_assets, "returns.png", include_bitcoin=False)
        plot_absolute_price_comparison(returns_df, "apt_absolute_price.png")
        
        # 5. 결과를 Excel로 저장
        print("\n[5단계] 결과 저장 중...")
        output_excel = "apt_returns_analysis.xlsx"
        with pd.ExcelWriter(output_excel, engine='openpyxl') as writer:
            returns_df.to_excel(writer, sheet_name='월별수익률', index=False)
            df_processed.to_excel(writer, sheet_name='전체데이터', index=False)
        print(f"결과 저장 완료: {output_excel}")
        
        print("\n" + "=" * 60)
        print("분석 완료!")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n오류 발생: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
