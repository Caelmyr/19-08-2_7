-- ============================================================
-- 回收站功能增量迁移脚本（适用于已部署、不便重建表的环境）
-- 全新部署直接使用 scripts/schema.sql 即可，无需执行本脚本。
--
-- 作用：为 documents 表增加软删除字段，删除改为 UPDATE 软删，
--       文档快照与 operations 历史完整保留，支持恢复与自动清理。
-- ============================================================
USE coedit;

ALTER TABLE documents
    ADD COLUMN deleted_at DATETIME NULL DEFAULT NULL COMMENT '删除时间（进入回收站时间），NULL表示未删除' AFTER updated_at,
    ADD COLUMN expires_at DATETIME NULL DEFAULT NULL COMMENT '回收站自动彻底删除时间，NULL表示不在回收站' AFTER deleted_at,
    ADD INDEX idx_deleted_at (deleted_at),
    ADD INDEX idx_expires_at (expires_at);
