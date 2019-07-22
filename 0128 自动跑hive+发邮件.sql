/////////////////////////////////////////////////////////////
定时调度任务：
50 08 * * * source /etc/profile && sh /home/fenghx/deepweb/order_track/run.sh >> /home/fenghx/deepweb/order_track/log 2>&1
00 09 * * 6 source /etc/profile && sh /home/fenghx/deepweb/gm_weekly/run.sh >> /home/fenghx/deepweb/gm_weekly/log 2>&1







用户所建立的crontab文件中，每一行都代表一项任务，每行的每个字段代表一项设置，它的格式共分为六个字段，前五段是时间设定段，第六段是要执行的命令段，格式如下：
minute hour day month week command

其中：
minute： 表示分钟，可以是从0到59之间的任何整数。
hour：表示小时，可以是从0到23之间的任何整数。
day：表示日期，可以是从1到31之间的任何整数。
month：表示月份，可以是从1到12之间的任何整数。
week：表示星期几，可以是从0到7之间的任何整数，这里的0或7代表星期日。
command：要执行的命令，可以是系统命令，也可以是自己编写的脚本文件。

/////////////////////////////////////////////////////////////
每日资源位产出 run.sh:

#!/usr/bin/sh
check_date=`date -d "1 day ago" +"%Y%m%d"`

hive -hivevar begin_date=$check_date -f '/home/fenghx/deepweb/order_track/order_track_daily.sql' > '/home/fenghx/data/order_track_daily$check_date'

wait
mv '/home/fenghx/data/order_track_daily$check_date' '/home/fenghx/data/order_track_daily$check_date.txt'

wait
sed -i '1i\日期\t资源位\t支付金额' '/home/fenghx/data/order_track_daily$check_date.txt'

file='/home/fenghx/data/order_track_daily$check_date.txt'

python '/home/fenghx/tool/mail.py' 




///////////////////////////////////////////////////////////////
团队邮件组 teammail.py:

#!/usr/bin/python
# -*- coding: utf-8 -*-
import smtplib
#
from email.header import Header
#发送字符串的邮件
from email.mime.text import MIMEText
#处理多种形态的邮件主体我们需要 MIMEMultipart 类
from email.mime.multipart import MIMEMultipart
#
from email.utils import parseaddr, formataddr
#
from email import encoders
#定义署名格式化函数
def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

#将读取txt内容输出到data中
f = open('/home/fenghx/data/order_track_daily$check_date.txt','r')
data = ''
while True:
	line = f.readline()
	data += line.strip() + '\n'
	if not line:
		break
f.close 

#设置邮箱服务器所需信息
from_addr = 'fenghongxiang@chuchujie.com'#邮件发送方邮箱地址
password = 'fhxboy2388373LEO'#密码
to_addr = 'fenghongxiang@chuchujie.com'#邮件接受方邮箱地址，多地址用[]包裹、逗号隔开

#设置email信息
#---------------------------发送字符串的邮件---------------------------
#邮件内容设置
message = MIMEText(data,'plain','utf-8')
#发送方信息
message['From'] = _format_addr(u'冯鸿翔 <%s>' % from_addr)
#接受方信息     
message['To'] = _format_addr(u'楚楚推产品运营中心 <%s>' % to_addr) 
#邮件主题       
message['Subject'] = Header('每日资源位产出数据', 'utf-8').encode()
#---------------------------------------------------------------------
 
 
#登录并发送邮件
try:
    server = smtplib.SMTP('smtp.exmail.qq.com')#QQ企业邮箱服务器地址
    server.login(from_addr,password)
    server.sendmail(from_addr, to_addr, message.as_string())
    print('发送成功')
    server.quit()
 
except smtplib.SMTPException as e:
	print('错误',e) #打印错误












参考：
f = open('/home/fhx/data/testdata.txt','r')
data = ''
for i in f:
    l = i.rsplit()
    t = '{0: <20}{1: >20}{2}'.format(l[0],l[1],l[2])
    data += str(t) + '\n'
f.close








参考：
#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import fnmatch
import smtplib
from email.mime.text import MIMEText
from email.header import Header

mail_host = "smtp.163.com"
user = "xxx@163.com"
passwd = "****"

send = 'xxx@163.com'
rec = ['yyy@163.com']
subject = 'python email test'

# 将读取的到不同文件的内容输出到data中
data = ''

path = "f:/nginx-out"
for _, _, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, "part-*"):
        with open(os.path.join(path, filename)) as src:
            data = data + ''.join(src.readlines())
src.close()

# print data

msg = MIMEText(data, 'plain', 'utf-8')
msg['Subject'] = Header(subject, 'utf-8')
# 此处需要按照个人邮件地址填写，否则会出现在垃圾箱里
msg['From'] = 'xxx@163.com'
msg['To'] = 'yyy@163.com'
try:
    smtp = smtplib.SMTP()
    smtp.connect(mail_host, 25)
    smtp.login(user, passwd)
    smtp.sendmail(send, rec, msg.as_string())
    print("发送邮件成功")
    smtp.quit()
except smtplib.SMTPException:
    print("Error: 无法发送邮件")
--------------------- 
作者：richard_zyq 
来源：CSDN 
原文：https://blog.csdn.net/liuzhuannianshao/article/details/78784484 
版权声明：本文为博主原创文章，转载请附上博文链接！















#!/usr/bin/python
# -*- coding: utf-8 -*-
import smtplib
#
from email.header import Header
#发送字符串的邮件
from email.mime.text import MIMEText
#处理多种形态的邮件主体我们需要 MIMEMultipart 类
from email.mime.multipart import MIMEMultipart
#
from email.utils import parseaddr, formataddr
#
from email import encoders
#
from optparse import OptionParser

#定义署名格式化函数
def _format_addr(s):
    name, addr = parseaddr(s)
    return formataddr((Header(name, 'utf-8').encode(), addr))

#设置邮箱服务器所需信息
from_email = 'fenghongxiang@chuchujie.com'#邮件发送方邮箱地址
password = 'fhxboy2388373LEO'#密码
to_addr = 'fenghongxiang@chuchujie.com'#邮件接受方邮箱地址，多地址用[]包裹、逗号隔开

#
if __name__ == "__main__":
    optparse.OptionParser()
    optparse.add_option("-n", "--from_name", dest = "from_name")
    optparse.add_option("-e", "--from_email", dest = "from_email")
    optparse.add_option("-s", "--subject", dest = "subject")
    optparse.add_option("-c", "--content", dest = "content")
    optparse.add_option("-T", "--to_list", dest = "to_list")
    optparse.add_option("-C", "--cc_list", dest = "cc_list")
    optparse.add_option("-a", "--attachment", dest = "attachment")
    optparse.add_option("-p", "--password", dest = "password")

    (options, args) = optparse.parse_args()

#设置email信息
#---------------------------发送字符串的邮件---------------------------
#邮件内容设置
message = MIMEText('Hello','plain','utf-8')
#发送方信息
message['From'] = _format_addr(u'冯鸿翔 <%s>' % from_email)
#接受方信息     
message['To'] = [options.to_list] 
#邮件主题       
message['Subject'] = Header(options.subject, 'utf-8').encode()
#---------------------------------------------------------------------

#登录并发送邮件
try:
    server = smtplib.SMTP('smtp.exmail.qq.com')#QQ企业邮箱服务器地址
    server.login(from_email,password)
    server.sendmail(from_email, options.to_list, message.as_string())
    print('发送成功')
    server.quit()

except smtplib.SMTPException as e:
    print('错误',e) #打印错误









python3 mail3.py -f "fenghongxiang@chuchujie.com" -t "fenghongxiang@chuchujie.com" -s "每日数据" -m "Hello"


./mail3.py -f "fenghongxiang@chuchujie.com" -t "fenghongxiang@chuchujie.com" -s "每日数据" -m "Hello"










//////////////////////////////////////////////////////////////////////

社群服务经理周数据 run.sh:

#!/usr/bin/sh

hive -f '/home/fenghx/deepweb/gm_weekly/gm_weekly.sql' > '/home/fenghx/deepweb/gm_weekly/gm_weekly'

wait
mv '/home/fenghx/deepweb/gm_weekly/gm_weekly' '/home/fenghx/deepweb/gm_weekly/gm_weekly.txt'

wait
sed -i '1i\服务经理id\t服务经理姓名\t手机号\t孵化人\t战区\t团队人数\t团队有订单人数\t团队订单数\t团队支付金额\t团队佣金\t团队推广有订单人数\t团队推广订单数\t团队推广支付金额\t团队推广佣金\t团队有招募人数\t团队招募新人数\t团队有分享人数\t团队分享次数\t人均分享次数\t团队分享率\t团队推广有订单率\t团队招募率' '/home/fenghx/deepweb/gm_weekly/gm_weekly.txt'

wait 
python '/home/fenghx/tool/t2x.py' /home/fenghx/deepweb/gm_weekly/gm_weekly.txt /home/fenghx/deepweb/gm_weekly/gm_weekly

wait
file='/home/fenghx/deepweb/gm_weekly/gm_weekly.xls'

python '/home/fenghx/tool/mail.py' \
-f "fenghongxiang@chuchujie.com" \
-t "dongchuxian@chuchujie.com","fenghongxiang@chuchujie.com" \
-s "服务经理周数据" \
-m "附件是本周服务经理维度数据，请查收。" \
-a $file

/////////////////////////////////////////////////////////////////////////////////
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



  