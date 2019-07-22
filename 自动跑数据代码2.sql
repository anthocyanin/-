
每周社群数据 run.sh:

#!/usr/bin/sh

hive -f '/home/gonghui/taskAll/shequn/shequn_weekdata.sql' > '/home/gonghui/taskAll/shequn/shequn_weekdata'

wait
mv '/home/gonghui/taskAll/shequn/shequn_weekdata' '/home/gonghui/taskAll/shequn/shequn_weekdata.txt'

wait
sed -i '1i\服务经理id\t服务经理姓名\t电话\t孵化人\t战区\t团队人数\t团队有订单人数\t团队订单数\t团队支付金额\t团队佣金\t团队推广有订单人数\t团队推广订单数\t团队推广支付金额\t团队推广佣金\t团队有招募人数\t团队招募新人数\t团队分享人数\t团队分享次数\t人均分享次数\t团队分享率\t团队推广有订单率\t团队招募率\t新人中又有招募的人数\t裂变率\t新人开单人数\t开单率' '/home/gonghui/taskAll/shequn/shequn_weekdata.txt'

wait 
python '/home/gonghui/tool/t2x.py' /home/gonghui/taskAll/shequn/shequn_weekdata.txt /home/gonghui/taskAll/shequn/shequn_weekdata

wait
file='/home/gonghui/taskAll/shequn/shequn_weekdata.xls'

python '/home/gonghui/tool/mail.py' \
-f "gonghui@chuchujie.com" \
-t "dongchuxian@chuchujie.com","gonghui@chuchujie.com" \
-s "每周社群数据" \
-m "附件是每周社群数据，请查收。" \
-a $file

      
/////////////////////////////////////////////////////////////////////////////////

15 15 * * * source /etc/profile && sh /home/gonghui/taskAll/shequn/run.sh >> /home/gonghui/taskAll/shequn/log 2>&1

/////////////////////////////////////////////////////////////////////////////////

