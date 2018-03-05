import django_tables2 as tables

class TweetTable(tables.Table):
    Variable_Name = tables.Column()
    Hashtag = tables.Column()
    Retweet_Of = tables.Column()
    Reply_Of = tables.Column()
    Quoted = tables.Column()
    Has_Mentioned = tables.Column()

    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

class UserTable(tables.Table):
    Variable_Name = tables.Column()
    UserId = tables.Column()
    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}

class RelationTable(tables.Table):
    Source = tables.Column()
    Relation_Ship = tables.Column()
    Destination = tables.Column()
    Begin = tables.Column()
    End = tables.Column()
    class Meta:
        attrs = {'class': 'paleblue','width':'200%'}
