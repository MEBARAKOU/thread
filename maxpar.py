# pprint est utilisé pour afficher les tableaux et dictionnaires (utile pour débugger)
### pprint.PrettyPrinter().pprint('test')
### import pprint

# la librairie threading permet de lancer des threads, cela permet de paralléliser les tâches
import threading
# la librairie copy et sa méthode deepcopy permettent de créer des copies rigoureuses d'un dictionnaire de tableaux
# (une copie "normale" utilisant dic.copy() ne copie pas le contenu des tableaux mais uniquement des références)
from copy import deepcopy
# Import des librairies permettant l'affichage du graphe d'exécution du système de parallélisme maximal.
import networkx as nx # etudier le graphe
import matplotlib.pyplot as plt # permet de tracer et visualiser des données sous formes de graphiques

# Définition de la classe Task
class Task:
    # Nom de la tâche
    name = ""
    # Nom des variables lues par la tâche
    reads = []
    # Nom des variables écrites par la tâche
    writes = []
    # Méthode liée à la tâche
    run = None

# Définition de la classe Worker²²²²
# Cette classe permet l'exécution parallèle d'un tâche passée en paramètre de son constructeur.
# Elle est aussi liée au système de tâches, afin de lancer les tâches suivantes lorsque la tâche courante est terminée.

class Worker(threading.Thread):
    # Constructeur de la classe Worker
    def __init__(self, task, ts ):
        # Appel du constructeur parent (de la classe Thread)
        threading.Thread.__init__(self)
        # Définition des variables d'instance
        self._task = task
        self._ts = ts

    # La méthode run() est héritée de la classe Thread.
    # Lorsqu'on appelle Worker.start() (c'est à dire, si un objet a est de type Worker, on appelle a.start()), cette méthode va être exécutée
    # en parallèle du fil d'exécution principal.
    # On exécute la tâche liée à l'objet, puis on notifie le système de tâches qu'elle est terminée et qu'on peut lancer les tâches suivantes
    # dont toutes les dépendances ont été exécutées (voir définition de la méthode remove_and_run_ready).
    def run(self):
        self._task.run()
        ts.remove_and_run_ready(self._task.name)

# http://theorangeduck.com/page/synchronized-python
# Cette méthode sera utilisée comme décorateur dans la méthode TaskSystem.remove_and_run_ready().
# En écrivant '@synchronized_method' au-dessus de l'en-tête de la fonction, cela permet d'éviter que deux fils d'exécution ne l'exécutent en même temps,
# ainsi garantissant l'absence d'accès concurrents.

### cette methode est utilisée pour forcer les threads à s'exécuter de manière synchrone
def synchronized_method(method):
    # Ce lock 'verrou' est commun à toutes les instances de TaskSystem qui serait amenée à utiliser le décorateur.
    outer_lock = threading.Lock()
    lock_name = "__"+method.__name__+"_lock"+"__"

    def sync_method(self, *args, **kws):
        with outer_lock:
            # Le verrou créé ici est spécifique à chaque instance de TaskSystem. Les deux lignes suivantes permettent de le lier à l'objet courant ('self')
            if not hasattr(self, lock_name): setattr(self, lock_name, threading.Lock())
            lock = getattr(self, lock_name)
            # On prend le verrou (si une instance l'a déjà pris, on attend qu'il soit libéré)
            # puis on exécute la méthode qui utilise le décorateur.
            with lock:
                return method(self, *args, **kws)

    return sync_method

# La classe TaskSystem modélise le système de tâches, et crée un système de parallélisme maximal.
class TaskSystem:

    # Constructeur de la classe TaskSystem.
    def __init__(self, task_list, dic_precedence):
        # Définition de la liste de tâches
        self._task_list = task_list
        # Définition des préférences de précédence
        self._dic_precedence = dic_precedence

        # Cette variable permet de signaler au fil d'exécution principal que le système a été complètement exécuté
        self._finished = False
        # Cette variable permet de savoir si le système est prêt à être démarré ou non
        self._ready = False

        # Génération du tableau d'interférences : voir définition de la méthode
        self._genere_tableau_interferences()

        # Vérification des entrées : voir définition de la méthode
        # Si les entrées sont correctes, on génère le dictionnaire d'exécution
        if self._verification_entrees():
            self._genere_dico_execution()

    # Cette méthode exécute diverses vérifications qui permettent de s'assurer de la validité du système (tâches et préférences)
    def _verification_entrees(self):
        # Vérification des tâches : retourne False si un nom est trouvé deux fois.
        task_names = [task.name for task in self._task_list]
        prec = list(self._dic_precedence)
        for i in range(0, len(task_names)):
            for j in range(i+1, len(task_names)):
                if task_names[i] == task_names[j]:
                    print("Le nom %s est dupliqué dans les noms de tâches, le système est invalide"%task_names[i])
                    return False
                if task_names[i] not in prec:
                   print("%s n'est pas présent dans le dictionnaire de précédence."%task_names[i])
                   return False
        # Vérification du dictionnaire de précédence
        # 1. Vérifie si toutes les tâches sont présentes et sans doublons.
        for i in range(0, len(prec)):
            for j in range(i+1, len(prec)):
                if prec[i] == prec[j]:
                    print("Le nom %s est dupliqué dans les préférences, le système est invalide"%task_names[i])
                    return False

            if prec[i] not in task_names:
                print("%s est dans le dictionnaire de précédence mais n'est pas dans les tâches connues."%prec[i])
                return False

        # 2. Vérifie qu'il n'y ait pas de dépendance cyclique (i.e. T1 attend T2 et T2 attend T1)
        for i in range(0, len(prec)):
            for j in range(i+1, len(prec)):
                if prec[i] in self._dic_precedence[prec[j]] and prec[j] in self._dic_precedence[prec[i]]:
                    print("Dépendence cyclique entre %s et %s"%(task_names[i], task_names[j]))
                    return False
        return True

    # Cette méthode génère un tableau 2D de booléens dont les index correspondent aux index des tâches dans le tableau de tâches. Si les tâches i et j du tableau de tâches interfèrent
    # (i.e. si elles accèdent toutes les deux à une variable commune), _tab_interference[i][j] et _tab_interference[j][i] vaudra True (matrice symétrique)
    def _genere_tableau_interferences(self):
        # Définition du tableau de base, remplie de valeurs False qui seront changées en cas d'interférence.
        # Ce tableau est stocké dans l'objet courant.
        self._tab_interference = [[False for i in range(0, len(self._task_list))] for j in range(0, len(self._task_list))]
        # On prend chaque tâche une par une et on analyse les tâches suivantes. Ainsi tous les couples de tâches seront analysés.
        for i in range(0, len(self._task_list)):
            for j in range(i + 1, len(self._task_list)):
                # On regarde si la tâche i lit une variable que la tâche j lit ou écrit. Si oui, il y a interférence.
                for var in self._task_list[i].reads:
                    if var in self._task_list[j].reads or var in self._task_list[j].writes:
                        self._tab_interference[i][j] = True
                        self._tab_interference[j][i] = True
                # On regarde si la tâche i écrit une variable que la tâche j lit ou écrit. Si oui, il y a interférence.
                for var in self._task_list[i].writes:
                    if var in self._task_list[j].reads or var in self._task_list[j].writes:
                        self._tab_interference[i][j] = True
                        self._tab_interference[j][i] = True

        return self._tab_interference

### Génère le dictionnaire d'exécution
    def _genere_dico_execution(self):
        self._dico_execution = {item:[] for item in self._dic_precedence.keys()}
        for i in range(0, len(self._task_list)):
            for j in range(0, len(self._task_list)):
                task = self._task_list[i]
                int_task = self._task_list[j] 
                if self._tab_interference[i][j]:
                    # print("Interference entre %s et %s"%(task.name, int_task.name))
                    if int_task.name in self._dic_precedence[task.name]:
                        self._dico_execution[task.name].append(int_task.name)
                else :
                    # self._dico_execution[task.name] = []
                    pass
        self._ready = True
        self._dico_execution_copy = deepcopy(self._dico_execution)
        return self._dico_execution

    # Cette méthode permet de lancer le système de tâches.
    def run(self):
        # Si le système n'est pas prêt (i.e. si il y a eu un problème à l'initialisation), on empêche le lancement
        if not self._ready:
            return False
        cp_dico = deepcopy(self._dico_execution)
        for elem in list(cp_dico):
            # Pour chaque élément du dictionnaire de départ, si une tâche n'a pas de précédence, on l'enlève du dictionnaire principal et on la lance
            if not cp_dico[elem]:
                self._dico_execution.pop(elem, None)
                self._start(elem)

        # Puis on attend que la variable _finished soit passée à True avant de quitter la méthode
        while not self._finished:
            pass

    # Cette méthode permet de lancer une tâche dans un fil d'exécution séparé (grâce à la classe Worker)
    def _start(self, task_name):
        task = self.get_task_by_name(task_name)
        worker = Worker(task, self)
        # Cette ligne permet de lancer la méthode run() de l'objet Worker, dans un fil d'exécution séparé
        worker.start()

    # Cette méthode retourne l'objet Task lié au nom donné en paramètre
    def get_task_by_name(self, task_name):
        for task in self._task_list:
            if task.name == task_name:
                return task
        return None


    # Cette méthode permet d'enlever une tâche du dictionnaire d'exécution avant de lancer celles qui sont prêtes (i.e. dont la précédence est vide)
    # Le décorateur @synchronized_method permet de limiter l'accès à la méthode à un seul thread à la fois, ainsi il n'y a pas de problèmes d'accès concurrents.
    @synchronized_method
    def remove_and_run_ready(self, task_name):
        # Copie du dictionnaire d'exécution
        keys = list(deepcopy(self._dico_execution))
        # Si il n'y a plus de tâches à effectuer, on passe la variable _finished à True pour signaler au fil principal que le système a été complètement exécuté
        if not keys:
            self._finished = True
       # Sinon, on supprime la tâche de toutes les précédences auxquelles elle fait partie puis on l'enlève du dictionnaire
        cp_dico = deepcopy(self._dico_execution)
        for elem in keys:
            # print(elem)
            if task_name in cp_dico[elem]:
                self._dico_execution[elem].remove(task_name)
                cp_dico[elem].remove(task_name)
            # Si une tâche n'a maintenant plus de précédence, on la lance
            if not cp_dico[elem]:
                self._dico_execution.pop(elem, None)
                self._start(elem)

        # print("Exiting remove for %s"%task_name)

    # Cette méthode permet d'afficher le graphe d'exécution
    def draw(self):
        if not self._ready :
            return False

        # La liste de noeuds correspond aux clés du dictionnaire d'exécution
        list_nodes = list(self._dico_execution_copy)
        # La liste d'arêtes correspond aux couples (tâche, tâche précédente):
        # pour chaque tâche, on crée un couple avec chaque tâche présente dans sa liste de précédence
        list_edges = []
        for i in range(0, len(list_nodes)):
            for j in range(i+1, len(list_nodes)):
                if(list_nodes[i] in self._dico_execution_copy[list_nodes[j]]):
                    list_edges.append((list_nodes[i], list_nodes[j]))

        # Définition d'un graphe orienté
        G=nx.DiGraph()
        # Ajout des noeuds
        G.add_nodes_from(list_nodes)
        # Ajout des arêtes
        G.add_edges_from(list_edges)
        # Disposition des noeuds
        nx_pos = nx.shell_layout(G)
        # Dessin du graphe
        labels = nx.draw_networkx(G, pos=nx_pos)
        # Sauvegarde du graphe dans un fichier
        plt.savefig("graph.png")
        # Affichage du graphe
        plt.show()

X = None
Y = None
Z = None

def runT1():
    global X
    X = 7

def runT2():
    global X
    X = 3

def runT3():
    global Y
    Y = 4


def runT4():
    global Z, X, Y
    Z = X + Y

def runT5():
    global Z


t1 = Task()
t1.name = "T1"
t1.writes = ["X"]
t1.run = runT1

t2 = Task()
t2.name = "T2"
t2.writes = ["X"]
t2.run = runT2

t3 = Task()
t3.name = "T3"
t3.writes = ["Y"]
t3.run = runT3

t4 = Task()
t4.name = "T4"
t4.reads = ["X", "Y"]
t4.writes = ["Z"]
t4.run = runT4

t5 = Task()
t5.name = "T5"
t5.reads = ["Z"]
t5.run = runT5


task_list = [t1, t2, t3, t4, t5]
# task_list = [t1, t3, t5]
# task_list = [t1, t2, t3,t4, t5]
dico_pref_prec = {"T1": [], "T2":["T1"], "T3":["T1"], "T4":["T2","T3"], "T5":["T1", "T3", "T2", "T4"]}
#dico_pref_prec = {"T1": [], "T3":["T1"], "T5":["T1", "T3"]}
# dico_pref_prec = {"T1": [], "T2":["T1"], "T3":["T1"], "T4":["T2","T3"], "T5":["T1", "T3", "T2", "T4"]}
ts = TaskSystem(task_list, dico_pref_prec)
ts.run()
print(Z)
ts.draw()