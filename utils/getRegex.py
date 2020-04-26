def get_regex(kwVar, blacklist=False):
    """
    Returns regex version of input
    """
    if kwVar != '':
        kwVarList = kwVar.split(',')
        kwList = []
        for kw in kwVarList:
            kwList.append(kw.strip())
        kwRegex = '|'.join(kwList)
        return kwRegex
    else:
        if blacklist:
            kwVar = 'thistextshouldneverbefound'
        else:
            kwVar = '.*'
    return kwVar