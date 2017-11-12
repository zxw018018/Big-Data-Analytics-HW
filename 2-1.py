
# coding: utf-8

# In[937]:


import graphdb_client
import logging 
import json


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s',)
g = graphdb_client.gc(host = 'http://localhost:8010')
    
# Load header vertex file which file is locate on the user machine
def load_movie_vertex():
    vertex_file_path='../data/movie_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "MOVIE"
    #in the content_type {"aaa":['aa','INT']}  
    #'aa' is the column name in the csv's header, 'aaa' is the property name you want to call in your graph
    
    content_type = [{"budget": ['budget', "DOUBLE"]},
    {"genres": ["genres", "STRING"]},{"keywords": ["keywords", "STRING"]},
    {"popularity": ["popularity", "FLOAT"]},{"revenue": ["revenue", "DOUBLE"]},
    {"runtime": ["runtime", "INT"]},{"title": ["title", "STRING"]},
    {"vote_average": ["vote_average", "FLOAT"]},{"vote_count": ["vote_count", "INT"]}]

    column_header_map = {
                "vertex_id": "id",
                "properties":content_type
            }


    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        content_type = content_type,
                        data_row_start = -1, 
                        data_row_end = -1)
    print rc
    
def load_actor_vertex():
    vertex_file_path='../data/actor_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "ACTOR"
    content_type = [{"actor_name": ['actor_name', "STRING"]},{"gender": ["gender", "INT"]}]

    column_header_map = {
                "vertex_id": "id",
                "properties":content_type
            }

    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        content_type = content_type,
                        data_row_start = -1, 
                        data_row_end = -1)
    print rc
def load_director_vertex():
    vertex_file_path='../data/director_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "DIRECTOR"
    content_type = [{"director_name": ['director_name', "STRING"]},{"gender": ["gender", "INT"]}]

    column_header_map = {
                "vertex_id": "director_id",
                "properties":content_type
    }

    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label, 
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        content_type = content_type,
                        data_row_start = -1, 
                        data_row_end = -1)
    
    print rc
    
# Load header edge file which file is locate on local machine
def load_act_edge():
    edge_file_path = '../data/actor_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "MOVIE"
    default_target_label = "ACTOR"
    default_edge_label = 'ACT'
    content_type = [{"order":['order', "INT"]}]
    edge_column_header_map = {
                "source_id": "source_id",
                "target_id":"target_id",
                "properties":content_type
            }

    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map,  
                      column_number_map=[{}],
                      data_row_start= -1, 
                      data_row_end= -1)
    print rc
    
def load_act_edge_reverse():
    edge_file_path = '../data/actor_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "ACTOR"
    default_target_label = "MOVIE"
    default_edge_label = 'ACT'
    content_type = [{"order":['order', "INT"]}]
    edge_column_header_map = {
                "source_id": "target_id",
                "target_id":"source_id",
                "properties":content_type
            }

    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map,  
                      column_number_map=[{}],
                      data_row_start= -1, 
                      data_row_end=-1)
    print rc
    
    
def load_dir_edge():
    edge_file_path = '../data/director_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "MOVIE"
    default_target_label = "DIRECTOR"
    default_edge_label = 'DIR'
    content_type = []
    edge_column_header_map = {
                "source_id": "source_id",
                "target_id":"target_id",
                "properties":content_type
        }
    
    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map, 
                      column_number_map={},

                      data_row_start= -1, 
                      data_row_end=-1)
    print rc
def load_dir_edge_reverse():
    edge_file_path = '../data/director_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "DIRECTOR"
    default_target_label = "MOVIE"
    default_edge_label = 'DIR'
    content_type = []
    edge_column_header_map = {
                "source_id": "target_id",
                "target_id":"source_id",
                "properties":content_type
        }
    
    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map, 
                      column_number_map={},

                      data_row_start= -1, 
                      data_row_end=-1)	
    print rc




# In[934]:


rc = g.set_current_graph('hw3_2')
print (rc)


# In[935]:


rc = g.delete_graph('hw3_2')
print(rc)


# In[936]:


rc = g.create_graph(graph_name = 'hw3_2')
print (rc)


# In[938]:


rc = g.set_current_graph('hw3_2')
print (rc)


# In[939]:


load_movie_vertex()


# In[940]:


load_actor_vertex()


# In[941]:


load_director_vertex()


# In[942]:


load_act_edge()
load_act_edge_reverse()


# In[943]:


load_dir_edge()
load_dir_edge_reverse()


# # Vertex Degree: Find out who is the most prolific actor

# In[944]:


import csv
with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/actor_vertice.csv") as f:
    list_actor_id = [row.split(',')[0] for row in f]
del list_actor_id[0] #delete 'id'


# In[945]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/actor_vertice.csv") as f:
    list_actor_name = [row.split(',')[1] for row in f]
del list_actor_name[0] #delete 'actor_name'


# In[946]:


print list_actor_id


# In[947]:


print list_actor_name


# In[948]:


print(len(list_actor_id))


# In[949]:


list_num_movie=[]

for i in list_actor_id:
    rc=g.get_edge_in_count(vertex_id=str(i), vertex_label='ACTOR', edge_label=['ACT'])
    act_num = json.loads(rc)["statistics"]["num_edges"]
    list_num_movie.append(act_num)


# In[950]:


print(list_num_movie)


# In[951]:


print(len(list_num_movie))


# In[952]:


max_act_movies = max(list_num_movie)
print(max_act_movies)   #number of movies of the most prolific actor acted


# In[953]:


max_indices = [i for i, j in enumerate(list_num_movie) if j == max_act_movies]
print(max_indices) #indices of max


# In[954]:


print(list_actor_id[max_indices[0]]) #id of max


# ## Here is the most prolific actor

# In[955]:


print(list_actor_name[max_indices[0]]) #name of max


# # Vertex Neighbors: Find the actor whose movies earn highest profit in total

# In[958]:


list_profit=[]
for i in list_actor_id:
    rc=g.get_neighbor_out(vertex_id=str(i), vertex_label='ACTOR', edge_label=['ACT'],neighbor_label=['MOVIE'],distinct=False)
    act_movie = json.loads(rc)['data']['vertices']
    profit = 0.0
    actor_index = 0
    for i in act_movie:
        if i['label']=='ACTOR':
            break
        else:
            actor_index = actor_index + 1
    del act_movie[actor_index]
    for j in act_movie:
        profit = j['properties'][4]['revenue'] - j['properties'][0]['budget'] + profit
    list_profit.append(profit)


# In[959]:


print(list_profit)


# In[960]:


print(len(list_profit))


# In[961]:


max_profit = max(list_profit)
print(max_profit)


# In[962]:


max_profit_indices = [i for i, j in enumerate(list_profit) if j == max_profit]
print(max_profit_indices)


# In[963]:


print(list_actor_id[max_profit_indices[0]])


# ## Here is the actor whose movies earn highest profit in total

# In[964]:


print(list_actor_name[max_profit_indices[0]])


# # Create subgraph

# In[675]:


rc1 = g.get_neighbor_out(vertex_id='770',vertex_label='MOVIE',edge_label=[],neighbor_label=[],distinct=True)
first_depth = json.loads(rc1)['data']['vertices']
print first_depth


# In[673]:


print json.loads(rc1)['statistics']


# In[748]:


first_actor = []
first_dir = []
for i in first_depth:
    if i['label']=='ACTOR':
        first_actor.append(i)
    else:
        if i['label']=='DIRECTOR':
            first_dir.append(i)


# In[749]:


print first_actor


# In[750]:


print first_dir


# In[751]:


second_depth_movie = []
for i in first_actor:
    rc2 = g.get_neighbor_out(vertex_id=i['id'],vertex_label='ACTOR',edge_label=[],neighbor_label=[],distinct=True)
    second_depth_movie.append(json.loads(rc2)['data']['vertices'])


# In[752]:


second_depth = []
for i in second_depth_movie:
    for j in i:
        if j['label']=='MOVIE':
            second_depth.append(j)


# In[753]:


print(second_depth)


# In[754]:


print(len(second_depth))


# In[755]:


second_depth_movie2 = []
for i in first_dir:
    rc2 = g.get_neighbor_out(vertex_id=i['id'],vertex_label='DIRECTOR',edge_label=[],neighbor_label=[],distinct=True)
    second_depth_movie2.append(json.loads(rc2)['data']['vertices'])


# In[756]:


print(second_depth_movie2)


# In[757]:


second_depth2=[]
for i in second_depth_movie2:
    for j in i:
        if j['label']=='MOVIE':
            second_depth2.append(j)


# In[759]:


print(second_depth2)


# In[760]:


print(len(second_depth2))
print(len(second_depth))


# In[761]:


for i in second_depth2:
    second_depth.append(i)
print(len(second_depth))


# In[762]:


third_depth_all = []
for i in second_depth:
    rc2 = g.get_neighbor_out(vertex_id=i['id'],vertex_label='MOVIE',edge_label=[],neighbor_label=[],distinct=True)
    third_depth_all.append(json.loads(rc2)['data']['vertices'])


# In[768]:


print third_depth_all


# In[776]:


third_actor = []
third_dir = []
length = 0
for i in third_depth_all:
    for j in i:
        length = length+1
        if j['label']=='ACTOR':
            third_actor.append(j)
        if j['label']=='DIRECTOR':
            third_dir.append(j)


# In[777]:


print(len(third_actor))
print(len(third_dir))


# In[778]:


sub_actor = first_actor+third_actor
sub_dir = first_dir+third_dir


# In[780]:


print(len(sub_actor))
print(len(sub_dir))


# In[799]:


sub_movie = second_depth


# In[800]:


print len(sub_movie)


# In[805]:


new_movie=[]
for i in sub_movie:
    if i not in new_movie:
        new_movie.append(i)
print len(new_movie)


# In[803]:


new_actor = []
for i in sub_actor:
    if i not in new_actor:
        new_actor.append(i)
print(len(new_actor))


# In[804]:


new_dir = []
for i in sub_dir:
    if i not in new_dir:
        new_dir.append(i)
print len(new_dir)


# In[811]:


new_movie_id = []
new_movie_label = []
for i in new_movie:
    new_movie_id.append(i['id'])
    new_movie_label.append(i['label'])
print(new_movie_id)
print(new_movie_label)


# In[851]:


new_actor_id = []
new_actor_label = []
for i in new_actor:
    new_actor_id.append(i['id'])
    new_actor_label.append(i['label'])
print(new_actor_id)


# In[850]:


new_dir_id = []
new_dir_label = []
for i in new_dir:
    new_dir_id.append(i['id'])
    new_dir_label.append(i['label'])
print(new_dir_id)


# In[825]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/movie_vertice.csv") as f:
    movie_id = [row.split(',')[2] for row in f]
print movie_id


# In[846]:


movie_index=[]
index = 0
for i in movie_id:
    if i in new_movie_id:
        movie_index.append(index)
    index = index+1  
print(movie_index)


# In[848]:


real_movie_index=[]
for i in movie_index:
    real_movie_index.append(i+1)
print(real_movie_index)


# In[852]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/actor_vertice.csv") as f:
    actor_id = [row.split(',')[0] for row in f]
print actor_id


# In[853]:


actor_index=[]
index = 0
for i in actor_id:
    if i in new_actor_id:
        actor_index.append(index)
    index = index+1  
print(actor_index)


# In[854]:


real_actor_index=[]
for i in actor_index:
    real_actor_index.append(i+1)
print(real_actor_index)


# In[859]:


print(new_dir_id)


# In[868]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/new_dir_vertice.csv",'wb') as f:
    writer = csv.writer(f,delimiter=',')
    writer.writerow(['director_id'])
    for i in new_dir_id:
        writer.writerow([int(i)]) 


# In[869]:


print(new_movie_id)


# In[870]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/new_movie_vertice.csv",'wb') as f:
    writer = csv.writer(f,delimiter=',')
    writer.writerow(['id'])
    for i in new_movie_id:
        writer.writerow([int(i)]) 


# In[871]:


print(new_actor_id)


# In[872]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/new_actor_vertice.csv",'wb') as f:
    writer = csv.writer(f,delimiter=',')
    writer.writerow(['id'])
    for i in new_actor_id:
        writer.writerow([int(i)]) 


# In[875]:


# movie(source_id), actor(target_id)
with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/actor_edge.csv") as f:
    movie_id = [row.split(',')[0] for row in f]
print movie_id


# In[876]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/actor_edge.csv") as f:
    actor_id = [row.split(',')[1] for row in f]
print actor_id


# In[877]:


actor_edge_index=[]
for i in range (len(movie_id)):
    if ((movie_id[i] in new_movie_id) and (actor_id[i] in new_actor_id)):
        actor_edge_index.append(i)


# In[884]:


print(actor_edge_index)
print(len(actor_edge_index))


# In[886]:


print(movie_id[actor_edge_index[119]])
print(actor_id[actor_edge_index[119]])


# In[881]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/new_actor_edge.csv",'wb') as f:
    writer = csv.writer(f,delimiter=',')
    writer.writerow(['source_id','target_id'])
    for i in actor_edge_index:
        writer.writerow([int(movie_id[i]),int(actor_id[i])])


# In[887]:


# movie(source_id), director(target_id)
with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/director_edge.csv") as f:
    movie_id = [row.split(',')[0] for row in f]
print movie_id


# In[888]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/director_edge.csv") as f:
    dir_id = [row.split(',')[1] for row in f]
print dir_id


# In[892]:


print int(dir_id[1])


# In[897]:


dir_edge_index=[]
for i in range (len(movie_id)):
    if ((movie_id[i] in new_movie_id) and (str(int(dir_id[i])) in new_dir_id)):
        dir_edge_index.append(i)


# In[898]:


print(dir_edge_index)
print(len(dir_edge_index))


# In[900]:


print(movie_id[dir_edge_index[11]])
print(dir_id[dir_edge_index[11]])


# In[901]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/new_dir_edge.csv",'wb') as f:
    writer = csv.writer(f,delimiter=',')
    writer.writerow(['source_id','target_id'])
    for i in dir_edge_index:
        writer.writerow([int(movie_id[i]),int(dir_id[i])])


# In[1016]:


rc = g.set_current_graph('hw3_2new')
print (rc)


# In[1017]:


rc = g.delete_graph('hw3_2new')
print (rc)


# In[1018]:


rc = g.create_graph('hw3_2new')
print (rc)


# In[1019]:


rc = g.set_current_graph('hw3_2new')
print (rc)


# In[1020]:


logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(lineno)s %(message)s',)
g = graphdb_client.gc(host = 'http://localhost:8010')
    
# Load header vertex file which file is locate on the user machine
def new_load_movie_vertex():
    vertex_file_path='../data/new_movie_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "MOVIE"
    #in the content_type {"aaa":['aa','INT']}  
    #'aa' is the column name in the csv's header, 'aaa' is the property name you want to call in your graph
    
    column_header_map = {
                "vertex_id": "id"
            }


    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        data_row_start = -1, 
                        data_row_end = -1)
    print rc
    
def new_load_actor_vertex():
    vertex_file_path='../data/new_actor_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "ACTOR"
    column_header_map = {
                "vertex_id": "id"
            }

    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label,  
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        data_row_start = -1, 
                        data_row_end = -1)
    print rc
def new_load_director_vertex():
    vertex_file_path='../data/new_dir_vertice.csv'
    has_header = 1
    column_delimiter = ','
    default_vertex_label = "DIRECTOR"

    column_header_map = {
                "vertex_id": "director_id"
    }

    rc = g.load_table_vertex(file_path = vertex_file_path,
                        has_header = has_header,
                        column_delimiter = column_delimiter, 
                        default_vertex_label = default_vertex_label, 
                        column_header_map = column_header_map, 
                        column_number_map=[{}],
                        data_row_start = -1, 
                        data_row_end = -1)
    
    print rc
    
# Load header edge file which file is locate on local machine
def new_load_act_edge():
    edge_file_path = '../data/new_actor_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "MOVIE"
    default_target_label = "ACTOR"
    default_edge_label = 'ACT'
    edge_column_header_map = {
                "source_id": "source_id",
                "target_id":"target_id"
            }

    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map,  
                      column_number_map=[{}],
                      data_row_start= -1, 
                      data_row_end= -1)
    print rc
    
def new_load_act_edge_reverse():
    edge_file_path = '../data/new_actor_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "ACTOR"
    default_target_label = "MOVIE"
    default_edge_label = 'ACT'
    edge_column_header_map = {
                "source_id": "target_id",
                "target_id":"source_id"
            }

    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map,  
                      column_number_map=[{}],
                      data_row_start= -1, 
                      data_row_end=-1)
    print rc
    
    
def new_load_dir_edge():
    edge_file_path = '../data/new_dir_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "MOVIE"
    default_target_label = "DIRECTOR"
    default_edge_label = 'DIR'
    content_type = []
    edge_column_header_map = {
                "source_id": "source_id",
                "target_id":"target_id"
        }
    
    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map, 
                      column_number_map={},
                      data_row_start= -1, 
                      data_row_end=-1)
    print rc
def new_load_dir_edge_reverse():
    edge_file_path = '../data/new_dir_edge.csv'
    has_header = 1
    column_delimiter = ','

    default_source_label = "DIRECTOR"
    default_target_label = "MOVIE"
    default_edge_label = 'DIR'
    content_type = []
    edge_column_header_map = {
                "source_id": "target_id",
                "target_id":"source_id"
        }
    
    rc = g.load_table_edge(file_path = edge_file_path,
                      has_header = has_header, 
                      column_delimiter= column_delimiter, 
                      default_source_label = default_source_label, 
                      default_target_label = default_target_label, 
                      default_edge_label = default_edge_label, 
                      column_header_map = edge_column_header_map, 
                      column_number_map={},
                      data_row_start= -1, 
                      data_row_end=-1)	
    print rc





# In[1021]:


rc = g.set_current_graph('hw3_2new')
print (rc)


# In[1022]:


new_load_movie_vertex()


# In[1023]:


new_load_actor_vertex()


# In[1024]:


new_load_director_vertex()


# In[1025]:


new_load_act_edge()
new_load_act_edge_reverse()


# In[1026]:


new_load_dir_edge()
new_load_dir_edge_reverse()


# # Find out who is the most important director  by measuring his/her betweenness

# In[1027]:


print(new_dir_id)


# In[1036]:


list_path =[]
i = 0
for v in new_movie_id:
    list_path.append([])
    for u in new_actor_id:
        rc = g.get_path(source_id=str(v),source_label='MOVIE',target_id=str(u),target_label='ACTOR',
                        edge_labels=[],depth=10)
        list_path[i].append(rc)
    i = i + 1


# In[1038]:


print(new_dir_id)


# In[1039]:


betweenness = []
for v in range(len(new_dir_id)):
    betweenness_tmp = 0.0
    pair = 0.0
    belong = 0.0
    for s in range(len(new_movie_id)):
        if (s!=v):
            for t in range(len(new_actor_id)):
                if ((t!=s) and (t!=v)):
                    rc = list_path[s][t]
                    if (json.loads(rc)['status']=='success'):
                        vertices = json.loads(rc)['data']['vertices']
                        for i in vertices:
                            if (i['id']==new_dir_id[v]):
                                belong = belong + 1
                    pair = pair + 1                  
    betweenness_tmp = belong/pair
    betweenness.append(betweenness_tmp)
print(betweenness)


# In[1044]:


max_betweenness_index = betweenness.index(max(betweenness))
print max_betweenness_index


# In[1046]:


print(new_dir_id[max_betweenness_index])


# In[1048]:


with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/director_vertice.csv") as f:
    list_director_id = [row.split(',')[0] for row in f]
with open("/Users/xinwei/Desktop/columbia/BigData/hw3/HW3/GraphDB/data/director_vertice.csv") as f:
    list_director_name = [row.split(',')[1] for row in f]


# # Here is the most important director by measuring betweenness

# In[1053]:


print list_director_name[list_director_id.index(new_dir_id[max_betweenness_index])]


# # Find out who is the most important director by measuring his/her closeness

# In[1057]:


closeness = []
for v in new_dir_id:
    distance = 0.0
    for u in new_dir_id:
        rc = g.get_path(source_id=str(v),source_label='DIRECTOR',target_id=str(u),target_label='DIRECTOR',
                        edge_labels=[],depth=10)
        status = json.loads(rc)['status']
        if (status=='success'):
            distance = distance + json.loads(rc)['statistics']['num_edges']
    closeness.append(1/distance)


# In[1058]:


print(closeness)


# In[1059]:


max_closeness = max(closeness)
print max_closeness


# In[1061]:


max_closeness_indices = [i for i, j in enumerate(closeness) if j == max_closeness]
print(max_closeness_indices)


# In[1070]:


dir1_id = new_dir_id[(max_closeness_indices[0])]
dir2_id = new_dir_id[(max_closeness_indices[1])]
print(dir1_id,dir2_id)


# # Here are the most important directors by measuring their closeness

# In[1072]:


print list_director_name[list_director_id.index(dir1_id)]
print list_director_name[list_director_id.index(dir2_id)]

