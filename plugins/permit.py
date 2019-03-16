from nonebot import on_command, CommandSession, permission as perm

@on_command('add', permission=perm.SUPERUSER)
async def add_p(session:CommandSession):
    with open('permit', 'r') as buf:
        perm_set = eval(buf.read())
    raw = session.ctx['raw_message'].split('add')[1].strip()
    perm_set.add(int(raw))
    with open('permit', 'w') as buf:
        buf.write(str(perm_set))
    
@on_command('rmv', permission=perm.SUPERUSER)
async def remove_p(session:CommandSession):
    with open('permit', 'r') as buf:
        perm_set = eval(buf.read())
    raw = session.ctx['raw_message'].split('rmv')[1].strip()
    perm_set.remove(int(raw))
    with open('permit', 'w') as buf:
        buf.write(str(perm_set))

def get_perm_usr():
    with open('permit', 'r') as buf:
        return eval(buf.read())

@on_command('ppm', permission=perm.SUPERUSER)
async def repr_(session:CommandSession):
    await session.send(str(get_perm_usr()))