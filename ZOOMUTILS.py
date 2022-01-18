#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
fonction utilitaire Zoom.us
'''



class ZoomUtils:
	def SetHeaders():
		""" Cette fonction prépare le headers pour les requêtes Zoom
		"""
		global headers
		global token
		with open('configzoom.json') as json_data_file:
			config = json.load(json_data_file)

		now = datetime.now()
		payload = {'iss': config['keys']['APIKey'], 'exp': (now + timedelta(hours=1))}
		#token =  str(jwt.encode(payload, config['keys']['APISecret']), 'utf-8')
		token =  jwt.encode(payload, config['keys']['APISecret'])
		headers = {
		'authorization': 'Bearer %s' % token,
		'content-type': 'application/json'
		}
		return headers
	def SetSavePath():
		""" Détermine et crée le dossier de savegarde
		"""
		global save_path
		with open('configzoom.json') as json_data_file:
			config = json.load(json_data_file)
		
		#récupère le dossier
		save_folder = config['settings']['save_folder']
	             
		save_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),save_folder)
		if not os.path.exists(save_path):
			os.makedirs(save_path)
		
		return save_path
	def GetSettingsParam(key):
		with open('configzoom.json') as json_data_file:
			config = json.load(json_data_file)
		settings = config['settings']
		value = settings.get(key)
		return value
		
	def create_connection(db_file):
		""" create a database connection to the SQLite database
			cified by db_file
		:param db_file: database file
		:return: Connection object or None
		"""
		conn = None
		try:
			conn = sqlite3.connect(db_file)
			return conn
		except Error as e:
			print(e)

		return conn

	def create_table(conn, create_table_sql):
		""" create a table from the create_table_sql statement
		:param conn: Connection object
		:param create_table_sql: a CREATE TABLE statement
		:return:
		"""
		try:
			c = conn.cursor()
			c.execute(create_table_sql)
		except Error as e:
			print('Erreur création de table :\n{}'.format(e))
		return
	def InsertSynapseS(conn,row):
		sql =''' INSERT INTO Cours (matricule, nom, prenom, email_eleve_etb, matriculepersonne, code_ue, intervenants, email_interv_etb, autres_intervenants, date, heuredeb, heurefin, groupes)
				 VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'''
		cur = conn.cursor()
		cur.execute(sql,row)
		conn.commit()
		return cur.lastrowid
	def GetZoomUserList(page):
		url = f"https://api.zoom.us/v2/users?page_number={page}&page_size=300&status=active"
		#next_page_token={page+1}&
		r = requests.get(url, headers=headers)
		if r.status_code == 200:
			return json.loads(r.content)
	def GetUser(userId):
		url = f"https://api.zoom.us/v2/users/{userId}"
		r = requests.get(url, headers=headers)
		if r.status_code == 200:
			return json.loads(r.content)
	def ListAllRecordings(userId,days = -30):
		if abs(days) > 30:
			iter = int(abs(days)/30)+1
			meetings = []
			for i in range (iter,0,-1):
				_from = datetime.now() + timedelta(days = -int(i*30))
				_to = _from + timedelta(days = 30)
				fromdate = _from.strftime('%Y-%m-%d')
				todate = _to.strftime('%Y-%m-%d')
				#print(f"{fromdate} - {todate}")
				url = f"https://api.zoom.us/v2/users/{userId}/recordings?trash_type=meeting_recordings&from={fromdate}&to={todate}&page_size=300"
				r = requests.get(url,headers=headers)
				if r.status_code == 200:
					recordlist = json.loads(r.content)
					meetings = meetings + recordlist['meetings']
					#print(meetings)
					if recordlist['total_records']>300:
						print(f"Attention, il y a {total_records} enregistrements, seul les 300 premiers sont traités")
					recordlist['meetings'] = meetings
				else:
					print (f'Erreur : {r.status_code} ')
					recordlist = None
		else:

			now = datetime.now() + timedelta(days = days)
			fromdate = now.strftime('%Y-%m-%d')
			now = datetime.now()
			todate = now.strftime('%Y-%m-%d')

			url = f"https://api.zoom.us/v2/users/{userId}/recordings?trash_type=meeting_recordings&from={fromdate}&to={todate}&page_size=300"
			#print(url)
			r = requests.get(url,headers=headers)
			if r.status_code == 200:
				recordlist = json.loads(r.content)
				if recordlist['total_records']>300:
						print(f"Attention il y a {total_records} enregistrements, seul les 300 premiers sont traités")
				#------
				# ATTENTION ICI, si la valeur de 'total_records' est >300 il y a d'autres pages à chager
				#------
			else:
					print (f'Erreur : {r.status_code} ')
					recordlist = None
		
		return recordlist
	def ExistsMeeting(conn, id):
		sql = """ SELECT id FROM recordings WHERE zoomid like '{}'; """
		cur = conn.cursor()
		cur.execute(sql.format(id))
		myresult = cur.fetchall()
		return len(myresult)
	def ExistsFile(conn, id):
		sql = """ SELECT zoomid  FROM recfiles 
				  WHERE zoomid like '{}';"""
		cur = conn.cursor()
		cur.execute(sql.format(id))
		myresult = cur.fetchall()
		if len(myresult) >0:
			return True
		else:
			return False
	def StoreRecordingDB(conn, meeting):
		"""	Enregistre les infos principales d'un meeting
		:param _meeting : Json meetings list
		:return: id
		"""
		#print (meeting)
		sql = """ SELECT id, download_ok FROM recordings WHERE uuid LIKE '{}';
				  """
		#sql = """ SELECT id, record_id FROM recordings WHERE recording_id LIKE '{}';"""
		cur = conn.cursor()
		cur.execute(sql.format(meeting['uuid']))
		myresult = cur.fetchall()
		if not myresult:
			recording = (meeting['uuid'],
						 meeting['id'],
						 meeting['account_id'],
						 meeting['host_id'], 
						 meeting['topic'], 
						 meeting['start_time'],
						 meeting['timezone'],
						 meeting['duration'],
						 meeting['total_size'], 
						 meeting['recording_count'])
			
			sql =''' INSERT INTO recordings (uuid,zoomid,account_id,host_id,topic,start_time,timezone,duration,total_size,recording_count)
					 VALUES (?,?,?,?,?,?,?,?,?,?)'''
			cur = conn.cursor()
			cur.execute(sql,recording)
			conn.commit()
			return cur.lastrowid
		else:
			return myresult[0]
	def StoreMediaDB(conn, media):
		"""	Enregistre les infos des medias 
		:param conn : informations de connection à la base de données
		:param media: json structure 'recordin_files'
		:return: id
		"""
		#print (media)
		#print(media['file_type'])
		if media['file_type'] != 'TIMELINE':
			sql = """ SELECT id, record_id FROM recfiles WHERE record_id LIKE '{}';"""
			cur = conn.cursor()
			cur.execute(sql.format(media['id']))
			myresult = cur.fetchall()

			# si le fichier n'y est pas on l'insère
			if len(myresult) == 0:
				file = (media['meeting_id'],
						 media['id'],
						 media['recording_start'],
						 media['recording_end'],
						 media['file_type'],
						 media['file_size'],
						 media['download_url'],
						 media['status'],
						 media['recording_type'])
				sql =''' INSERT INTO recfiles (recordings_uuid,record_id,recording_start,recording_end,file_type,file_size,download_url,status,recording_type)
						 VALUES (?,?,?,?,?,?,?,?,?)'''
				cur = conn.cursor()
				cur.execute(sql,file)
				conn.commit()
				return cur.lastrowid
			else:
				#print("Pas d'enregistrement")
				#print(sql.format(media['id']))
				return 0
		return 0

	def StoreMedia(conn, media):
		"""	Enregistre les infos des medias 
		:param conn : informations de connection à la base de données
		:param media: json structure 'recordin_files'
		:return: id
		"""
		#print (media)
		if media['file_type'] == 'TIMELINE':
			file = (media['meeting_id'],
					 'xxx',
					 media['recording_start'],
					 media['recording_end'],
					 media['file_type'],
					 '0',
					 media['download_url'],
					 'xxx',
					 'json',
					 2,
					 'xxx')
			sql =''' INSERT INTO recfiles (recordings_uuid,record_id,recording_start,recording_end,file_type,file_size,download_url,status,recording_type,downloaded,uploaded)
					 VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
			cur = conn.cursor()
			cur.execute(sql,file)
			conn.commit()
			return cur.lastrowid

		elif media['file_type'] != 'TIMELINE':
			file = (media['meeting_id'],
					 media['id'],
					 media['recording_start'],
					 media['recording_end'],
					 media['file_type'],
					 media['file_size'],
					 media['download_url'],
					 media['status'],
					 media.get('recording_type','>>INCONNU<<'))	#media['recording_type'])
			if media.get('recording_type','>>INCONNU<<') != '>>INCONNU<<':
				sql =''' INSERT INTO recfiles (recordings_uuid,record_id,recording_start,recording_end,file_type,file_size,download_url,status,recording_type)
						 VALUES (?,?,?,?,?,?,?,?,?)'''
				cur = conn.cursor()
				cur.execute(sql,file)
				conn.commit()
				return cur.lastrowid
			else:
				return 0
		return 0

	def GetFilesToDownload(conn):
		""" Retourne la liste de fichiers à télécharger
		:param: conn les information de la connexion
		:return: liste au format json
		"""
		sql = """SELECT uuid, topic, recording_start, download_url,record_id,file_type FROM recordings
				 INNER JOIN recfiles ON uuid = recordings_uuid
				 WHERE (file_type like 'MP4' or file_type like 'CHAT') and downloaded IS NULL
				 ORDER BY uuid asc;
				 """
		cur = conn.cursor()
		cur.execute(sql)
		myresult = cur.fetchall()
		files = []
		for count, row in enumerate(myresult):
			file = {}
			file['uuid'] = row[0]
			file['topic'] = row[1]
			file['recording_start'] = row[2]
			file['download_url'] = row[3]
			file['id'] = row[4]
			file['file_type'] = row[5]
			files.append(file)
		return files

	def GetMeetingList(conn, date_debut = '', date_fin = ''):
		if date_debut == '':
			date_debut = datetime.now()
		if date_fin == '':
			date_fin = date_debut + timedelta(days=7)

		sql = ''' SELECT meeting_id, topic,start_time FROM zoommeetings 
				  WHERE date(start_time) >= date('{}') AND date(start_time) < date('{}');'''.format(date_debut,date_fin)
		cur = conn.cursor()
		cur.execute(sql)
		myresult = cur.fetchall()
		return myresult

	def SaveFileFromUrl(url,output):
		ZoomUtils.SetHeaders()
		destination = os.path.join(save_path, output)
		#print ("Téléchargement : {}".format(output) )
		url = url + '?access_token={}'.format(token)
		#print (f"{output}: {url}")
		start_time = time_.clock()
		try:
			r = requests.get(url)
			end_time = time_.clock()
			# Recupère HTTP meta-data
			if r.status_code == 200 and 'Content-length' in r.headers:
				#if 'Content-length' in r.headers:
				print (f"{output}\tDurée: {end_time - start_time:2.2f} sec\tTaille: {r.headers['Content-length']:12s}\t{int(r.headers['Content-length']) / ((end_time - start_time) *1e6):3.2f} Mb/s" )
				#else:
				#print (f"{output}\tDurée: {end_time - start_time:2.2f} sec\tTaille: ??\t?? Mb/s" )

				try:
					with open(destination, 'wb') as f:
						f.write(r.content)
					return True
				except IOError:
					print ("Erreur d'écriture du fichier {}".format(destination))
			else:
				if r.status_code != 200:
					print (f"Erreur : {output} {r.status_code}")
					return False

			return False

		except Exception as e:
			print (f"{output}\tErreur : {e} ")
			return False


	def UpdateRecfilesDB(conn,record_id):
		# met à jour l'enregistrement 
		sql = "UPDATE recfiles SET downloaded=1 where record_id like '{}' ;"
		cur = conn.cursor()
		cur.execute(sql.format(record_id))
		conn.commit()
		return
	
	def UpdateRecordingsDB(conn):
		sql = ''' SELECT uuid, recording_count from recordings
				  WHERE download_ok is null;'''

		cur = conn.cursor()
		cur.execute(sql)
		myresult = cur.fetchall()

		for record in myresult:
			uuid = record[0]
			count = record[1]

			sql = ''' SELECT count(*) from recfiles
					  WHERE recordings_uuid = '{}';'''.format(uuid)
			cur = conn.cursor()
			cur.execute(sql)
			result = cur.fetchone()

			# si tout a été téléchargé
			if result[0] == count:
				sql = """UPDATE recordings
						 SET download_ok = 1
						 WHERE uuid like '{}' ;"""
				cur = conn.cursor()
				cur.execute(sql.format(uuid))
				conn.commit()
		return
		
	def EndMeeting(meeting_id):
		ZoomUtils.SetHeaders()
		url = "https://api.zoom.us/v2/meetings/{}/status".format(meeting_id)
		payload = '{"action":"end"}'
		r = requests.put(url,payload,headers=headers)
		if r.status_code == 204:
			#print('{} closed'.format(meeting_id))
			logging.info('%s fermé', meeting_id)
		else:
			#print ('Erreur fermeture meeting : {} - {}',format(r.status_code,r.content))
			logging.warning('Erreur fermeture meeting : %s / %s - %s',meeting_id, r.status_code,r.content)
		return
		
	def DeleteRecording(meetingId):
		ZoomUtils.SetHeaders()
		#print(url)
		if "/" in meetingId:
			#double encodage
			meetingId = urllib.parse.quote_plus(meetingId)
			meetingId = urllib.parse.quote_plus(meetingId)
			#print(meetingId)
		url = "https://api.zoom.us/v2/meetings/{}/recordings?action=trash".format(meetingId)
		
		try:
			r = requests.delete(url,headers=headers)
			if r.status_code != 204:
				print(meetingId, r.status_code, r.content)
		except HTTPError as http_err:
			print(f'HTTP error occurred: {http_err}')  # Python 3.6
			response = None
		except Exception as err:
			print(f'Other error occurred: {err}')  # Python 3.6
			response = None
		return r.status_code

	def EditMeeting(meetingId, params):
		headers = ZoomUtils.SetHeaders()
		body = json.dumps(params)
		#body = params
		url = 'https://api.zoom.us/v2/meetings/{}'.format(meetingId)
		r = requests.patch(url, body, headers=headers)

		if r.status_code == 204:
			print('Meeting {} actualisé'.format(meetingId))
		else:
			print('Meeting {} Erreur: {}'.format(meetingId,r.status_code))
			print(r.content)
			exit()
		return

	def EditWebinar(webinarId, params):
		headers = ZoomUtils.SetHeaders()
		body = json.dumps(params)
		#body = params
		url = f'https://api.zoom.us/v2/webinars/{webinarId}'
		r = requests.patch(url, body, headers=headers)

		if r.status_code == 204:
			print(f'Webinar {webinarId} actualisé')
		else:
			print(f'Webinar {webinarId} Erreur: {r.status_code}')
			print(r.content)
			exit()
		return

	def RecoverSingleRecording(meetingId,recordingId):
		ZoomUtils.SetHeaders()
		if "/" in meetingId:
			#double encodage
			meetingId = urllib.parse.quote_plus(meetingId)
			meetingId = urllib.parse.quote_plus(meetingId)
		if "/" in recordingId:
			#double encodage
			recordingId = urllib.parse.quote_plus(recordingId)
			recordingId = urllib.parse.quote_plus(recordingId)
		url = "https://api.zoom.us/v2/meetings/{}/recordings/{}/status".format(meetingId,recordingId)
		payload = "{\"action\":\"recover\"}"
		try:
			r = requests.put(url,payload,headers=headers)
			if r.status_code != 204:
				print(meetingId, r.status_code, r.content)
		except HTTPError as http_err:
			print(f'HTTP error occurred: {http_err}')  # Python 3.6
			response = None
		except Exception as err:
			print(f'Other error occurred: {err}')  # Python 3.6
			response = None
		return r.status_code

	def RecoverMeetingRecordings(meetingId):
		ZoomUtils.SetHeaders()
		if "/" in meetingId:
			#double encodage
			meetingId = urllib.parse.quote_plus(meetingId)
			meetingId = urllib.parse.quote_plus(meetingId)
		url = "https://api.zoom.us/v2/meetings/{}/recordings/status".format(meetingId)
		payload = "{\"action\":\"recover\"}"
		try:
			r = requests.put(url,payload,headers=headers)
			if r.status_code != 204:
				print(meetingId, r.status_code, r.content)
		except HTTPError as http_err:
			print(f'HTTP error occurred: {http_err}')  # Python 3.6
			response = None
		except Exception as err:
			print(f'Other error occurred: {err}')  # Python 3.6
			response = None
		return r.status_code		

	def GetMeetingDetails(meetingId):
		headers = ZoomUtils.SetHeaders()
		
		if isinstance(meetingId,str):
			if "/" in meetingId:
				#double encodage
				meetingId = urllib.parse.quote_plus(meetingId)
				meetingId = urllib.parse.quote_plus(meetingId)
		
		url = 'https://api.zoom.us/v2/meetings/{}'.format(meetingId)
		r = requests.get(url, headers=headers)
		if r.status_code == 200:
			#print('Meeting {} Erreur: {}'.format(meetingId,r.status_code))
			#print(r.content)
			return r.content
		else:
			print('Meeting {} Erreur: {}'.format(meetingId,r.status_code))
			print(r.content)

		return 

	def GetRecordingsbyUUID(uuid):
		#/meetings/{meetingId}/recordings
		if "/" in uuid:
			#double encodage
			uuid = urllib.parse.quote_plus(uuid)
			uuid = urllib.parse.quote_plus(uuid)
		ZoomUtils.SetHeaders()
		url = f"https://api.zoom.us/v2/meetings/{uuid}/recordings"
		try:
			r = requests.get(url,headers=headers)
			if r.status_code != 200:
				print(uuid, r.status_code, r.content)
			else:
				return json.loads(r.content)
		except HTTPError as http_err:
			print(f'HTTP error occurred: {http_err}')  # Python 3.6
			return None
		except Exception as err:
			print(f'Other error occurred: {err}')  # Python 3.6
			return None
		return None

	def StoreRecording(conn, meeting):
		"""	Enregistre les infos principales d'un meeting
		:param _meeting : Json meetings list
		:return: id
		"""

		recording = (meeting['uuid'],
					 meeting['id'],
					 meeting['account_id'],
					 meeting['host_id'], 
					 meeting['topic'], 
					 meeting['start_time'],
					 meeting['timezone'],
					 meeting['duration'],
					 meeting['total_size'], 
					 meeting['recording_count'])
		try:
			sql =''' INSERT INTO recordings (uuid,zoomid,account_id,host_id,topic,start_time,timezone,duration,total_size,recording_count)
				 VALUES (?,?,?,?,?,?,?,?,?,?)'''
			cur = conn.cursor()
			cur.execute(sql,recording)
			conn.commit()
			return cur.lastrowid
		except Exception as e:
			conn.rollback()
			raise e

	def ExportLinkMoodle(ZoomMeeting, ue, groupe, intitule_occur,btext=False):
		""" Génère un fichier Json avec les données à exporter
		:return: json
		"""
		"""
		{
		"titre": "CODE UE",
		"date": "09/03/2020",
		"debut": "19:00",
		"fin": "20:00",
		"duree": "60",
		"url": "https://ecolepolytechnique.zoom.us/j/589596145?pwd=RjNoMTlQNnRLZlg2b2M2SWhzR2VPdz09",
		"url_prof": "https://ecolepolytechnique.zoom.us/s/589596145?zak=eyJ6bV9za20iOiJ6bV9vMm0iLCJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJhdWQiOiJjbGllbnQiLCJ1aWQiOiJ1eVRYZjlVTVRkQ2pULVlURmkzYVVnIiwiaXNzIjoid2ViIiwic3R5IjoxMDAsIndjZCI6ImF3MSIsImNsdCI6MCwic3RrIjoiVXBmdFB2c3BRc1h2aGxJSWE2YmJyUW9fZUNGRGo5My1Ta2p4d282YjMyOC5CZ1VzZHpKWmQzcDBWMm8yTjIxcFIwMWhVa1V3Wm13dk5ESkZSVk5tV205UlVEUnpRMHh3Ym5CRk9EUnFUVDFBWVRKbFlUVXhPR000T1dJNVpqUTNObVpsWTJGbU5qVm1ZMkpoWkdFMU1XUmhZbUUxWWpRMVlXUm1PRFZqTm1RME5EbGxNelV4Wm1aaU5HRTFOVEZoT1FBTU0wTkNRWFZ2YVZsVE0zTTlBQU5oZHpFIiwiZXhwIjoxNTg0MzY3MTUzLCJpYXQiOjE1ODQzNTk5NTMsImFpZCI6IlNQc3FveHdqVHBxTy1TTGh0bDMwcnciLCJjaWQiOiIifQ.7cdsP7oZF1gpwtOUSM67wyY51b2Of3ock5YKK854uUU"
		}
		"""
		utc = pytz.utc
		ueLink = {}
		ueLink['titre'] = ZoomMeeting['topic']
		start_time = utc.localize(datetime.strptime(ZoomMeeting['start_time'],'%Y-%m-%dT%H:%M:%SZ'))
		timestamp = datetime.timestamp(start_time)
		ueLink['date'] = timestamp
		#
		#ueLink['debut'] =
		#ueLink['fin'] =
		ueLink['duree'] = ZoomMeeting['duration']
		ueLink['ue'] = ue
		ueLink['groupe'] = groupe
		ueLink['intitule_occur'] = intitule_occur
		ueLink['url'] = ZoomMeeting['join_url']
		ueLink['url_prof'] = ZoomMeeting['start_url']
		ueLink['meeting_id'] = ZoomMeeting['id']
		ueLink['meeting_pwd'] = ZoomMeeting['password']
		ueLink['display_intro'] = btext
		phoneNumbers = []
		for phn in ZoomMeeting['settings']['global_dial_in_numbers']:
			phoneNumbers.append(phn['number'])
		ueLink['phone'] = phoneNumbers
		return ueLink
					

import os
import jwt, json, sys
import datetime
from datetime import datetime
from datetime import timedelta  
#import time
import time as time_
import requests
from requests.exceptions import HTTPError
import pytz
import string
import urllib.parse 
import logging

import sqlite3
from sqlite3 import Error

