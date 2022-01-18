#!/usr/bin/env python3
# coding: utf-8
import os
import sys
import csv
from datetime import date
from datetime import timedelta
from datetime import datetime
from ZOOMUTILS import ZoomUtils


def main(argv):
	import_file = argv[0]

	forced = False
	if len (argv) >1:
		if argv[1] == '-forced':
			forced = True

	global conn
	RED = '\033[91m'
	GREEN = '\033[92m'
	GREEN = '\033[92m'
	BLUE = '\033[94m'
	PURPLE = '\033[95m'
	TEAL = '\033[96m'
	DEFAULT = '\033[0m'
	# créer la base de données dans le dossier du CSV
	database = r'SynapseS_tempo_amphi'+ datetime.strftime(datetime.now(),"%Y%m%d")+'.db'
	# Chercher le chemin du CSV et Concatener path . BDD
	database = os.path.join(os.path.dirname(os.path.abspath(import_file)),database)

	# Crée la base de donnée (ou l'ouvre)
	conn = ZoomUtils.create_connection(database)
	
	"""
		PHASE 1 : INTEGRATION DE SYNAPSES
	"""
	# créer la table de Cours
	#"code_ue";"date";"heuredeb";"heurefin";"groupes";"intitule";"intitule_occur";"intervenants";"email_intervenant"
	sql_create_table_cours = ''' CREATE TABLE IF NOT EXISTS cours_simple (
									matricule text NULL,
									code_ue text NULL,
									"date" text NULL,
									heuredeb text NULL,
									heurefin text NULL,
									groupes text NULL,
									intitule text NULL,
									intitule_occur text NULL,
									intervenants text NULL,
									email_interv_etb text NULL,
									salle text NULL)
									'''
	ZoomUtils.create_table(conn,sql_create_table_cours)

	if forced:
		sql ='''DELETE FROM cours_simple; '''
		print('Vide la table cours_simple')
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()

	# Vérification que la table est vide
	sql = '''SELECT count(*) FROM cours_simple; '''
	cur = conn.cursor()
	cur.execute(sql)
	val = cur.fetchone()
	if val[0] == 0 :
		synapse = []
		# injecter les données
		with open(import_file) as csv_file:
			csv_reader = csv.reader(csv_file, delimiter=';')
			line_count = 0
			for row in csv_reader:
				if line_count == 0:
					# on vérifie que les entêtes correspondent au format attendu
					if len(row) != 11:
						print (f"Le fichier n'a pas le nombre de colonne attendu. ({row} {len(row)}/11)")
						exit()
					line_count += 1
				else:
					if len(row) != 11:
						print(f"Nombre de champs invalide ({row}/10) - Erreur ligne: {line_count}")
					#on importe le fichier
					matricule = row[0]
					code_ue = row[1]
					intervenants = row[2]
					email_interv_etb = row[3]
					dte = row[4]
					intitule = row[5]
					heuredeb = row[6]
					heurefin = row[7]
					groupes = row[8]
					intitule_occur = row[9]

					data =(matricule,code_ue, dte, heuredeb, heurefin, groupes, intitule, intitule_occur, intervenants, email_interv_etb )
					synapse.append(data)
					#ZoomUtils.InsertSynapseS(conn, data)
					
					line_count += 1
			print(f'{line_count} enregistrements traités.')

		# Inject le contenu du fichier CSV dans la table
		sql =''' INSERT INTO cours_simple (matricule, code_ue, "date", heuredeb, heurefin, groupes, intitule, intitule_occur, intervenants, email_interv_etb)
		 VALUES (?,?,?,?,?,?,?,?,?,?)'''

		try:
			cur = conn.cursor()
			cur.executemany(sql, synapse)
			conn.commit()
		except Exception as e:
			print(f'Erreur INSERT: {e}')
			conn.rollback()
			exit()
	else:
		print(RED+"**\nLa table cours n'était pas vide. Pas d'importation des données\n**"+DEFAULT)
	

	# Suppression des cours antérieurs au prochain lundi
	sql = ''' DELETE FROM cours_simple WHERE date("date") < date('now','weekday 1') '''
	if not forced:
		try:
			print('Suppression des cours passés')
			cur = conn.cursor()
			cur.execute(sql)
			conn.commit()
		except Exception as e:
			print(f'Erreur: {e}')
			conn.rollback()

	
	# Suppression des cours supérieurs au lundi de la semaine suivantes
	today = date.today()
	#today = datetime.strptime("2020-03-25", "%Y-%m-%d")
	lundi = today + timedelta( ((0-today.weekday()) % 7) + 7 )
	#print(lundi)

	sql = ''' DELETE FROM cours_simple WHERE date("date") >= date('{}') ;'''
	try:
		print('Suppression des cours après le {}'.format(lundi.strftime('%d/%m/%Y')))
		cur = conn.cursor()
		cur.execute(sql.format(lundi.strftime('%Y-%m-%d')))
		conn.commit()
	except Exception as e:
		print(RED+f'Erreur: {e}'+DEFAULT)
		conn.rollback()
	

	"""
		PHASE 2 : PREPARATION DE LA TABLE AGENDA
	"""
	sql_create_table_agenda = ''' CREATE TABLE IF NOT EXISTS agenda (
											  id_agenda integer PRIMARY KEY AUTOINCREMENT,
											  code_ue text,
											  "date" text,
											  heuredeb text,
											  heurefin text,
											  groupes text,
											  live integer,
											  traite integer,
											  idzoommeeting integer,
											  intitule text,
											  intitule_occur text,
											  intervenants text,
											  webinar integer DEFAULT 0,
											  UNIQUE (code_ue, "date", heuredeb, heurefin, groupes,intitule,intitule_occur,intervenants)
											);'''
	ZoomUtils.create_table(conn,sql_create_table_agenda)

	# Vérification que la table est vide
	sql = '''SELECT count(*) FROM agenda; '''
	cur = conn.cursor()
	cur.execute(sql)
	val = cur.fetchone()
	if val[0] == 0 or forced:
		sql = ''' INSERT INTO agenda (code_ue, "date", heuredeb, heurefin,intitule, intitule_occur, groupes, intervenants)
					select distinct code_ue, "date", heuredeb, heurefin, intitule, intitule_occur, lower(groupes) as groupes, intervenants from cours_simple; '''
		try:
			print('Insertion Agenda 1')
			cur = conn.cursor()
			cur.execute(sql)
			conn.commit()
		except Exception as e:
			print(RED+f'Erreur: {e}'+DEFAULT)
			conn.rollback()
		"""
		# traitement des cours particuliers
		sql = ''' INSERT INTO agenda (code_ue, "date", heuredeb, heurefin, intitule, intitule_occur, groupes)
					select distinct code_ue, "date", heuredeb, heurefin, intitule, intitule_occur, lower(groupes) as groupes from cours
					where groupes like '' and code_ue IN ('MAP531', 'MAA302', 'HSS586','INF473');'''
		try:
			print('Insertion Agenda 2')
			cur = conn.cursor()
			cur.execute(sql)
			conn.commit()
		except Exception as e:
			print('Erreur: {}'.format(e))
			conn.rollback()
		"""
	else:
		print(RED+"La table agenda n'était pas vide"+DEFAULT)
	
	"""
	ICI on peut faire un test du nombre d'élève pour savoir si un webinar est nécessaire
		SELECT code_ue,lower(groupes),date,heuredeb,count(*) FROM cours_simple 
		group by code_ue,date,	heuredeb
		having lower(groupes) = 'tous les étudiants' and count(*) > 300

	"""
	"""
		RECHERCHE DES COURS POUR WEBINAR
	"""
	sql ='''CREATE TEMPORARY TABLE temp_table AS
			SELECT code_ue,lower(groupes) as groupes ,date,heuredeb,heurefin, count(*) as nb FROM cours_simple 
			group by code_ue,date,	heuredeb
			having lower(groupes) = 'tous les étudiants' and count(*) > 350
			order by date, heuredeb; '''
	cur.execute(sql)

	sql ='''SELECT a.id_agenda, nb from temp_table as t
			inner join agenda as a on t.code_ue = a.code_ue and t.groupes = a.groupes and t.date = a.date and t.heuredeb = a.heuredeb '''
	cur.execute(sql)
	rows = cur.fetchall()
	for r in rows:
		id = r[0]
		nb = r[1]
		sql =f'''UPDATE agenda Set webinar = 1 where id_agenda = {id} '''
		cur.execute(sql)
		conn.commit()
	print(f"{len(rows)} Webinaires détectés")

	"""
		RECHERCHE DES WEBINARS SIMULTANES MEME UE -> VISIO
	"""
	sql = '''SELECT distinct id_agenda, code_ue, "date", heuredeb, heurefin 
			FROM agenda 
			WHERE webinar = 1 
			ORDER by 'date' asc, heuredeb asc, heurefin asc, id_agenda asc;'''

	mycursor = conn.cursor()
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	print(f'Transformation des WEBINARS en VISIO pour {len(myresult)} cours ')

	for myCours in myresult:
		# affecte les variables pour plus de lisibilité
		id_agenda = myCours[0]
		code_ue 	= myCours[1]
		dte = myCours[2]
		heuredeb = myCours[3]
		heurefin = myCours[4] 

		sql = f'''SELECT count (distinct id_agenda) 
				FROM agenda 
				WHERE (date = '{dte}') and heuredeb = '{heuredeb}' and code_ue = '{code_ue}' and webinar = 1; '''
		mycursor.execute(sql)
		nbWebinar = mycursor.fetchone()
		if nbWebinar[0]>1:
			sql = f'''update agenda
						set webinar = 0
						where (date = '{dte}') and heuredeb = '{heuredeb}' and code_ue = '{code_ue}' and webinar = 1; '''
			mycursor.execute(sql)
			conn.commit()

	#####
	# Suppression des lignes sans groupe faisant doublon
	# 1/12/2020 ajout de ||intitule_occur dans les paramètres de comparaison, cas des cours de langues
	#####
	sql = ''' DELETE from agenda
				 where groupes = '' and (code_ue||date||heuredeb||intitule_occur in (SELECT code_ue||date||heuredeb||intitule_occur nb from agenda group by date, code_ue, heuredeb, heurefin, intitule_occur  having count(*) >1) );'''	
	try:
		print('Suppression des doublons groupes vides')
		cur = conn.cursor()
		cur.execute(sql)
		conn.commit()
		print('{} lignes supprimées'.format(cur.rowcount))
	except Exception as e:
		print(RED+f'Erreur: {e}'+DEFAULT)
		conn.rollback()
	

	"""
	#
	# Ce bloc de code commenté est remplacé par celui dessous (groupes uniques et multiples)
	#
	sql = '''SELECT * FROM agenda 
				where code_ue = 'PHY361'and groupes like '%$$%' and groupes not like 'tous%'
				order by date, heuredeb, groupes; '''
	cur.execute(sql)
	rows = cur.fetchall()
	if len(rows)>0:
		sql =''' DELETE from agenda
				 WHERE code_ue = 'PHY361'and groupes not like '%$$%' and groupes not like 'tous%'; '''
		try:
			print('Suppression des doublons PHY361')
			cur = conn.cursor()
			cur.execute(sql)
			conn.commit()
			print(f'{cur.rowcount} lignes supprimées')
		except Exception as e:
			print(RED+f'Erreur: {e}'+DEFAULT)
			conn.rollback()
	"""

	
	# Je cherche tous les groupes qui sont à la fois unique et multiples
	# Même créneau pour le PCC et n groupes d'élèves
	sql = '''SELECT id_agenda, code_ue, date, heuredeb, groupes,intitule,intitule_occur from agenda 
			where code_ue in (Select distinct code_ue from agenda where groupes like '%$$%')
			and groupes not like '%$$%'
			and groupes not like 'tous%'; '''
	cur.execute(sql)
	rows = cur.fetchall()

	inutile = 0
	for r in rows:
		id_agenda = r[0]
		code_ue = r[1]
		dte = r[2]
		heuredeb = r[3]
		groupes = r[4]
		intitule = r[5]
		intitule_occur = r[6]

		# Je cherche si le groupe multiple correspond bien à la même heure de debut
		sql = f'''SELECT * from agenda where code_ue ='{code_ue}' and groupes like '%{groupes}%' and groupes like '%$$%' and date ='{dte}' and heuredeb = '{heuredeb}' ; '''
		cur.execute(sql)
		rows2 = cur.fetchall()

		if len (rows2) >0:
			#ici on peut supprimer la ligne
			#print(f'{code_ue} {groupes} {dte} {heuredeb} {intitule} {intitule_occur} : {rows2}')
			try:
				sql = f'''Delete from agenda where id_agenda = {id_agenda}; '''
				cur.execute(sql)
				conn.commit()
				inutile +=1
			except Exception as e:
				print(f'Erreur de suppression {code_ue} {groupes} {dte} {heuredeb}')
				conn.rollback()

	print(f'Nombre de meetings groupés supprimé : {inutile}')	
	



	"""
		PHASE 3 : INTEGRATION DES VUES
	"""
	"""
	sql_v1 = ''' CREATE VIEW IF NOT EXISTS  FilesToUpload as
					SELECT record_id, zoomid,recording_start,topic, f.uploaded,r.upload_ok, f.file_type from recfiles as f
					inner join recordings as r on r.uuid = f.recordings_uuid
					where uploaded is null and file_type ='MP4'
					order by start_time;'''
	sql_v2 = ''' CREATE VIEW IF NOT EXISTS  RecordingFilesByClass as
					select code_ue, date, heuredeb, licence, recording_count, recording_type, record_id,status, downloaded, uploaded from RecordsFromAgenda as r
					inner join recfiles as f on uuid = f.recordings_uuid
					order by date, heuredeb,code_ue, f.id;
					'''
	sql_v3 = ''' CREATE VIEW IF NOT EXISTS  RecordsFromAgenda as
					select id_Agenda, code_ue, date, heuredeb, groupes, 'licencezoom'||(live+1)||'@polytechnique.fr' as licence, z.uuid, meeting_id, r.duration, r.recording_count, r.download_ok  from agenda as a 
					inner join zoommeetings as z on a.idzoommeeting = z.id
					inner join recordings as r on z.meeting_id = r.zoomid;'''

	print('Création des vues')
	try:
		print('1')
		cur = conn.cursor()
		cur.execute(sql_v1)
		conn.commit()
	except Exception as e:
		print(RED+f'Erreur: {e}'+DEFAULT)
		conn.rollback()

	try:
		print('2')
		cur = conn.cursor()
		cur.execute(sql_v2)
		conn.commit()
	except Exception as e:
		print(RED+f'Erreur: {e}'+DEFAULT)
		conn.rollback()

	try:
		print('3')
		cur = conn.cursor()
		cur.execute(sql_v3)
		conn.commit()
	except Exception as e:
		print(RED+f'Erreur: {e}'+DEFAULT)
		conn.rollback()				
	"""

	# Creation de la table email
	sql_create_table_email = ''' CREATE TABLE IF NOT EXISTS email_enseignants
								( 	id integer primary key AUTOINCREMENT,
									code_ue text,
									intervenants text,
									email text, 
									intitule_occur text,
									UNIQUE (code_ue, intervenants, intitule_occur));'''
	ZoomUtils.create_table(conn,sql_create_table_email)


	sql = ''' SELECT distinct  code_ue, intervenants, email_interv_etb AS 'email', intitule_occur FROM cours_simple
				WHERE  email_interv_etb != '' and code_ue not like 'SP%' and code_ue not like 'EP%'
				ORDER BY code_ue ASC;'''
				
	cur.execute(sql)
	rows = cur.fetchall()
	data =[]
	for row in rows:
		code_ue = row[0]
		intervenants = row[1]
		email = row[2]
		intitule_occur =row[3]

		if ';' in email:
			emails = email.split(';')
			for e in emails:
				if '@polytechnique.edu' in e:
					email = e
		elif '\n' in email:
			emails = email.split('\n')
			#print(emails)
			for e in emails:
				if '@polytechnique.edu' in e:
					#print(e)
					email = e
		data.append((code_ue,intervenants,email,intitule_occur))

	sql =''' INSERT OR REPLACE INTO email_enseignants (code_ue,intervenants,email,intitule_occur)
			VALUES (?,?,?,?)'''
	try:
		print('Création emails')
		cur = conn.cursor()
		cur.executemany(sql,(tuple(data)))
		conn.commit()
	except Exception as e:
		print(RED+f'Erreur emails : {e}'+DEFAULT)
		conn.rollback()

	"""
		PHASE 4 : CALCUL DE LA LICENCE ZOOM
	"""
	# Liste de tous les cours
	#sql = '''SELECT distinct id_agenda, code_ue, "date", heuredeb, heurefin, groupes, intitule_occur  FROM agenda WHERE code_ue NOT LIKE 'SP%' AND code_ue NOT LIKE 'AP-%' AND code_ue NOT LIKE 'EP%' ORDER by 'date' asc, heuredeb asc, heurefin asc, id_agenda asc;'''
	#DoNotGenerateFor = " code_ue NOT LIKE 'SP%' AND code_ue NOT LIKE 'AP-%' AND code_ue NOT LIKE 'EP%' AND code_ue NOT IN ('INF471C','INF533','INF535','INF557','INF472D','INF473X','CSE207', 'INF411','HSS413C', 'HSS413D', 'INF645', 'INF545', 'INF536-SEM','MIE630', 'CHI572', 'INF530','INF566','INF586','CSE207','INF564')"
	DoNotGenerateFor = " code_ue NOT LIKE 'SP%' AND code_ue NOT LIKE 'AP-%' AND code_ue NOT LIKE 'EP%' AND code_ue NOT IN ('CHI572','CSE207','CSE207','HSS413C','HSS413D','INF411','INF471C','INF472D','INF473X','INF530','INF533','INF535','INF536-SEM','INF545','INF557','INF566','INF586','INF645','LAN572gESP','MEC582','MIE630')"
	sql = '''SELECT distinct id_agenda, code_ue, "date", heuredeb, heurefin, groupes, intitule_occur  FROM agenda WHERE {} AND webinar = 0 ORDER by 'date' asc, heuredeb asc, heurefin asc, id_agenda asc;'''.format(DoNotGenerateFor)

	mycursor = conn.cursor()
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	print(f'Calcul des licences pour {len(myresult)} cours ')

	max_riched = False
	# parcours la liste
	for myCours in myresult:
		#print(myCours)
		# affecte les variables pour plus de lisibilité
		id_agenda = myCours[0]
		code_ue 	= myCours[1]
		dte = myCours[2]
		heuredeb = myCours[3]
		heurefin = myCours[4]
		
		# recherche les cours actifs au démarrage d'un autre avec 14 minutes de marge
		#sql = '''SELECT live from agenda where ("date" = '{}') and time('{}') Between time(heuredeb) and time(heurefin) and live not null and code_ue not like 'SP%' AND code_ue NOT LIKE 'AP-%' AND code_ue NOT LIKE 'EP%' order by live asc;'''.format(dte, heuredeb) 
		#sql = '''SELECT live from agenda where ("date" = '{}') and time('{}') Between time(heuredeb, '-14 minutes') and time(heurefin) and live not null and code_ue not like 'SP%' AND code_ue NOT LIKE 'AP-%' AND code_ue NOT LIKE 'EP%' order by live asc;'''.format(dte, heuredeb) 
		sql = '''SELECT live from agenda where ("date" = '{}') and time('{}') Between time(heuredeb, '-14 minutes') and time(heurefin) and live not null and {} ORDER BY live asc;'''.format(dte, heuredeb,DoNotGenerateFor)  

		#print(sql)
		mycursor.execute(sql)
		sqllives = mycursor.fetchall()

		#cré un tableau de live actifs
		lives =[]
		for l in sqllives:
			lives.append(l[0])
		#print(lives)

		#recherche la premiere licence libre
		free = None
		max_riched = False
		for l in range(0,81):
			if l not in lives:
				free = l
				break
		# Limite maximum atteint sortie du programme
		if free == None:
			max_riched = True
			print (RED+f"*** Problème : Limite atteinte le {dte} {heuredeb} - {heurefin} : {code_ue} \n{lives}"+DEFAULT)
			#exit()
		else:
			#print (id_agenda, free)

			# met à jour l'enregistrement 
			sql = "UPDATE agenda SET Live={} where id_agenda={} ;"
			mycursor.execute(sql.format(free, id_agenda))
			conn.commit()

	"""
		PHASE 5 : CALCUL DE LA LICENCE WEBINAR
	"""
	# Liste de tous les cours
	sql = '''SELECT distinct id_agenda, code_ue, "date", heuredeb, heurefin, groupes, intitule_occur  FROM agenda WHERE webinar = 1 ORDER by 'date' asc, heuredeb asc, heurefin asc, id_agenda asc;'''

	mycursor = conn.cursor()
	mycursor.execute(sql)
	myresult = mycursor.fetchall()
	print(f'Calcul des licences Webinar pour {len(myresult)} cours ')

	max_riched = False
	# parcours la liste
	for myCours in myresult:
		# affecte les variables pour plus de lisibilité
		id_agenda = myCours[0]
		code_ue 	= myCours[1]
		dte = myCours[2]
		heuredeb = myCours[3]
		heurefin = myCours[4]
		
		# recherche les cours actifs au démarrage d'un autre avec 14 minutes de marge
		sql = f'''SELECT live from agenda where ("date" = '{dte}') and time('{heuredeb}') Between time(heuredeb, '-14 minutes') and time(heurefin) and live not null ORDER BY live asc;'''  

		#print(sql)
		mycursor.execute(sql)
		sqllives = mycursor.fetchall()

		#cré un tableau de live actifs
		lives =[]
		for l in sqllives:
			lives.append(l[0])
		#print(lives)

		#recherche la premiere licence libre
		free = None
		max_riched = False
		for l in range(-1,-3,-1):
			if l not in lives:
				free = l
				break
		# Limite maximum atteint 
		if free == None:
			max_riched = True
			print (RED+f"*** Problème : Limite WEBINAR atteinte le {dte} {heuredeb} - {heurefin} : {code_ue} \n{lives}"+DEFAULT)
		else:
			#print (id_agenda, free)

			# met à jour l'enregistrement 
			sql = f"UPDATE agenda SET Live={free} where id_agenda={id_agenda} ;"
			mycursor.execute(sql)
			conn.commit()


	sql = ''' SELECT max(live) from agenda;'''
	mycursor.execute(sql)
	maxLive = mycursor.fetchone()
	conn.close()

	print(f'Licence maximum utilisée : {maxLive[0]}')
	print('Etape suivante : injection des données dans la base principale puis création des meetings Zoom')
	if max_riched:
		print("### Suite du processus arreté ###")
		exit(1)

if __name__ == '__main__':

	os.chdir(os.path.dirname(os.path.join(os.getcwd(),__file__)))
	localdir = os.path.dirname(os.path.join(os.getcwd(),__file__))
	localdir = os.path.abspath(os.path.join(localdir,"../Zoom"))

	folder = "Extraction_"+str(datetime.now().year)+str(datetime.now().isocalendar()[1]) # dump_YearWeeknumber
	savepath = os.path.join(localdir, folder)
	savedfilepath = os.path.join(savepath, "export.csv")
	if os.path.exists(savedfilepath) and len(sys.argv) < 2:
		main([f'{os.path.relpath(savedfilepath)}'])

	elif len(sys.argv) < 2:
		print("Erreur: Un fichier d'import doit être passé en paramètre.")
		exit()
	else:
		main(sys.argv[1:])

