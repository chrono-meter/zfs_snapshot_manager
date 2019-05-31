# zfs set com.sun:auto-snapshot=true pool/dataset
# crontab -l
@hourly /path/to/zfs_snapshot_manager.py
