freq = [
    'Giao dịch ngày n-1 ingest trước 12h ngày n',
    'Ngày: dump cuối ngày n-1/hoặc đầu ngày n ingest trước 12h ngày n',
    'Giao dịch ngày n-1 cập nhật ngày n',
    'Giao dịch. tháng t tính cước tháng t-1 cập nhật khoảng 15 tháng t+1',
    'Dữ liệu ngày n-2 cập nhật ngày n',
]

timestamp_column = "ingestion_time"  # Replace with the actual timestamp column name
cutoff_time = "12:00:00"  # Cutoff time for data readiness

tables = [
    'blueinfo_ocs_air',
    'blueinfo_ocs_crs_usage',
    'blueinfo_voice_msc',
    'blueinfo_voice_volte',
    'blueinfo_ggsn',
    'blueinfo_ccbs_ct_no',
    'blueinfo_ccbs_ct_tra',
    'blueinfo_ccbs_cv207',
    'blueinfo_ccbs_spi_3g_subs',
    'blueinfo_vascdr_2friend_log',
    'blueinfo_vascdr_brandname_meta',
    'blueinfo_vascdr_udv_credit_log',
    'blueinfo_vascdr_utn_credit_log',
    'blueinfo_vascloud_da',
    'blueinfo_tac_gsma',
    # 'prepaid_and_danhba',
    'blueinfo_smrs_dwd_geo_mtd_1',
    'blueinfo_smrs_dwd_geo_mtd_2',
    'blueinfo_smrs_dwd_geo_mtd_3',
    'blueinfo_smrs_dwd_geo_mtd_4',
    'blueinfo_ocs_sdp_subscriber',
    'blueinfo_device',
]