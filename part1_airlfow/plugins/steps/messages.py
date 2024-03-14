import os
from airflow.providers.telegram.hooks.telegram import TelegramHook
from dotenv import load_dotenv

load_dotenv()

tele_token = os.environ.get('TELEGRAM_TOKEN')
tele_chat = os.environ.get('TELECHAT_ID')

def send_telegram_success_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token=tele_token,
                        chat_id=tele_chat)
    dag = context['dag']
    run_id = context['run_id']
    
    message = f'Исполнение DAG {dag} с id={run_id} прошло успешно!' # определение текста сообщения
    hook.send_message({
        'chat_id': tele_chat,
        'text': message
    })

def send_telegram_failure_message(context):
    hook = TelegramHook(telegram_conn_id='test',
                        token=tele_token,
                        chat_id=tele_chat)
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']
    message = f'Исполнение дага {run_id} с {task_instance_key_str} прошло неуспешно'
    
    hook.send_message({
        'chat_id': tele_chat,
        'text': message
    })