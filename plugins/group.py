from nonebot import on_notice, NoticeSession, on_request, RequestSession, get_bot
import re

pattern = re.compile('[0-9]+')

@on_notice('group_increase')
async def _(session: NoticeSession):
    if session.ctx['group_id'] == 868085398:
        await session.send('欢迎新朋友～')

@on_request('group')
async def get_group_request(session: RequestSession):
    if session.ctx['group_id'] == 868085398:
        uid = session.ctx['user_id']
        cmm = session.ctx['comment']
        c = cmm.split('答案：')[1].strip()
        if not c:
            await session.reject('请填写验证信息')
        else:
            await session.approve()
            bot = get_bot()
            await bot.set_group_card(group_id=868085398, user_id=uid, card=c)