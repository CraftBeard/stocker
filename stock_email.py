import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
import stock_config as sc
import datetime

email_config = sc.EMAIL_CONFIG

def send_email():
    # create message object instance
    msg = MIMEMultipart()

    # setup the parameters of the message
    password = email_config['password']
    msg['From'] = email_config['from']
    msg['To'] = ", ".join(email_config['to'])
    msg['Subject'] = email_config['subject']
    smtp_server = email_config['smtp_server']
    smtp_port = email_config['smtp_port']

    # attach the image to the message
    with open("stocks.png", 'rb') as f:
        img_data = f.read()
    image = MIMEImage(img_data, name="stocks.png")
    image.add_header('Content-ID', '<image1>')
    # msg.attach(image)

    # add html email body
    today = datetime.date.today().strftime("%Y-%m-%d")
    with open("stock_values.csv", 'r') as f:
        stock_values = f.read()
    html = """
    <html>
      <head></head>
      <body>
        <p>Hi!</P>
        <p>Sent on: {}</p>
        <table>
          <thead>
            <tr>
              <th>证券代码</th>
              <th>证券名称</th>
              <th>最新收盘价</th>
              <th>跳楼度(越低越好)</th>
              <th>近3m最低价</th>
              <th>近3m最低价日期</th>
              <th>近6m最低价</th>
              <th>近6m最低价日期</th>
              <th>近12m最低价</th>
              <th>近12m最低价日期</th>
              <th>近24m最低价</th>
              <th>近24m最低价日期</th>
            </tr>
          </thead>
          <tbody>
            {% for stock in stock_values.itertuples() %}
            <tr>
              <td>{{ stock[1] }}</td>
              <td>{{ stock[2] }}</td>
              <td>{{ stock[3] }}</td>
              <td>{{ stock[4] }}</td>
              <td>{{ stock[5] }}</td>
              <td>{{ stock[6] }}</td>
              <td>{{ stock[7] }}</td>
              <td>{{ stock[8] }}</td>
              <td>{{ stock[9] }}</td>
              <td>{{ stock[10] }}</td>
              <td>{{ stock[11] }}</td>
              <td>{{ stock[12] }}</td>
            </tr>
            {% endfor %}
          </tbody>
        </table>
        <img src="cid:image1">
      </body>
    </html>
    """.format(today, stock_values))
    body = MIMEText(html, 'html')
    msg.attach(body)

    result=True
    try:
        server=smtplib.SMTP_SSL(smtp_server, smtp_port)  # connect to smtp server
        server.ehlo() # open connection
        server.login(msg['From'], password) # login to email account
        server.sendmail(msg['From'], msg['To'], msg.as_string()) # send email
        server.quit() # close connection
    except Exception as e:
        result=False
        print(e) # print error message if email sending fails
    return result # return True if email sent successfully, False otherwise

send_email() # call send_email function
