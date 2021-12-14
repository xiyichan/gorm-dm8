# gorm-dm8

gorm达梦数据库8适配驱动

连接的DSN请参考 【DM8程序员手册.pdf】

# 已发现BUG
1. 字符串用text不能正常用sql查询，但是可以写入，请定义的时候填入size