from nonebot import on_command, CommandSession, permission
import requests

@on_command('is_vpn', permission=permission.SUPERUSER)
async def is_vpn(session:CommandSession):
    try:
        req = requests.get('http://10.1.64.154/', timeout=7)
        await session.send('True')
    except TimeoutError:
        await session.send('False')

@on_command('菜单', only_to_me=False)
async def menu(session:CommandSession):
    await session.send('[CQ:image,file=bot/menu.png]')

def args_parser(args:str):
    process = dict()
    arg_parts = args.split('--')
    for i in arg_parts:
        if i:
            if '=' in i:
                left, right = i.split('=')
                process[left.strip().lower()] = eval(right)
            else:
                process[i.strip().lower()] = True
    return process