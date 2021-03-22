# from slacker import Slacker
#
# from rhipe_crawler_src.envlist import slack_api_token
#
# slacker = Slacker(slack_api_token)
#
#
# def send_text_msg(msg, channel):
#     return slacker.chat.post_message(channel=channel, text=msg, as_user=True)
#
#
# def send_msg_dict(msg: dict):
#     return slacker.chat.post_message(**msg)
#
#
# def file_upload(file, channel):
#     slacker.files.upload(file, channels=channel)