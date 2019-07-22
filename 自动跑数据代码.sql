
每日一级二级类目毛利额数据 run.sh:

#!/usr/bin/sh

hive -f '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.sql' > '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit'

wait
mv '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit' '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.txt'

wait
sed -i '1i\一级类目\t二级类目\t供货价\t佣金\t楚币\t支付金额\t毛利额\t毛利率' '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.txt'

wait 
python '/home/gonghui/tool/t2x.py' /home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.txt /home/gonghui/taskAll/grossprofit/daliy_Gross_Profit

wait
file='/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.xls'

python '/home/gonghui/tool/mail.py' \
-f "gonghui@chuchujie.com" \
-t "tangdong@chuchujie.com","gonghui@chuchujie.com" \
-s "每日一级二级类目毛利额数据" \
-m "附件是每日一级二级类目毛利额数据，请查收。" \
-a $file

/////////////////////////////////////////////////////////////////////////////////

20 07 * * * source /etc/profile && sh /home/gonghui/taskAll/grossprofit/run.sh >> /home/gonghui/taskAll/grossprofit/log 2>&1

/////////////////////////////////////////////////////////////////////////////////

python '/home/gonghui/tool/mail.py' \
-f "gonghui@chuchujie.com" \
-t "tangdong@chuchujie.com","gonghui@chuchujie.com" \
-s "每日一级二级类目毛利额数据" \
-m "附件是每日一级二级类目毛利额数据，请查收。" \
-a '/home/gonghui/taskAll/grossprofit/daliy_Gross_Profit.xls'


通用自动发送邮件 mail.py:

#!/usr/bin/python
# -*- coding: utf-8 -*-
 
import smtplib
import getopt
import sys
import os
 
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders

def send_mail(mail_from, mail_to, subject, msg_txt, files=[]):
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = mail_from
    msg['To'] = mail_to
 
    # Create the body of the message (a plain-text and an HTML version).
    #text = msg
    html = msg_txt
 
    # Record the MIME types of both parts - text/plain and text/html.
    #part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')
 
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    #msg.attach(part1)
    msg.attach(part2)
 
    #attachment
    for f in files:
        #octet-stream:binary data
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(f, 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)
 
    # Send the message via local SMTP server.
    # s = smtplib.SMTP('localhost')
    s = smtplib.SMTP_SSL( 'smtp.exmail.qq.com', 465 )
    s.login('fenghongxiang@chuchujie.com','fhxboy2388373LEO')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
 
    mailto_list = mail_to.strip().split(",")
    if len(mailto_list) > 1:
        for mailtoi in mailto_list:
            s.sendmail(mail_from, mailtoi.strip(), msg.as_string())
    else:
        s.sendmail(mail_from, mail_to, msg.as_string())
    s.quit()
    return True
def main():
    files = []
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:t:s:m:a:")
        for op, value in opts:
            if op == "-f":
                mail_from = value
            elif op == "-t":
                mail_to = value
            elif op == "-s":
                subject = value
            elif op == "-m":
                msg_txt = value
            elif op == "-a":
                files = value.split(",")
    except getopt.GetoptError:
        print(sys.argv[0] + " : params are not defined well!")
    print mail_from, mail_to, subject, msg_txt
    if files:
        send_mail(mail_from, mail_to, subject, msg_txt, files)
    else:
        send_mail(mail_from, mail_to, subject, msg_txt)
 
if __name__ == "__main__":
    main()

////////////////////////////////////////////////////////////
txt转换成xls格式 t2x.py

#!/usr/bin/python
# -*- coding: utf-8 -*-

import datetime
import time
import os
import sys
import xlwt #需要的模块

def txt2xls(filename,xlsname):  #文本转换成xls的函数，filename 表示一个要被转换的txt文本，xlsname 表示转换后的文件名
    print ('converting xls ... ')
    f = open(filename)   #打开txt文本进行读取
    x = 0                #在excel开始写的位置（y）
    y = 0                #在excel开始写的位置（x）
    xls=xlwt.Workbook(encoding = 'utf-8')
    sheet = xls.add_sheet('sheet1',cell_overwrite_ok=True) #生成excel的方法，声明excel
    while True:  #循环，读取文本里面的所有内容
        line = f.readline() #一行一行读取
        if not line:  #如果没有内容，则退出循环
            break
        for i in line.split('\t'):#读取出相应的内容写到x
            item=i.strip()
            sheet.write(x,y,item)
            y += 1 #另起一列
        x += 1 #另起一行
        y = 0  #初始成第一列
    f.close()
    xls.save(xlsname+'.xls') #保存

if __name__ == "__main__":
    filename = sys.argv[1]
    xlsname  = sys.argv[2]
    txt2xls(filename,xlsname)








python3 t2x.py /home/fhx/data/testdata.txt  ABC

ds>=from_unixtime(unix_timestamp()-3600*24*3,'yyyyMMdd')
and
ds<=from_unixtime(unix_timestamp()-3600*24*1,'yyyyMMdd')



  