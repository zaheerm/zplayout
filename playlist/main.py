# -*- Mode: Python; test-case-name: flumotion.test.test_common_gstreamer -*-
# vi:si:et:sw=4:sts=4:ts=4
#
# Flumotion - a streaming media server
# Copyright (C) 2004,2005,2006,2007 Fluendo, S.L. (www.fluendo.com).
# All rights reserved.

# This file may be distributed and/or modified under the terms of
# the GNU General Public License version 2 as published by
# the Free Software Foundation.
# This file is distributed without any warranty; without even the implied
# warranty of merchantability or fitness for a particular purpose.
# See "LICENSE.GPL" in the source distribution for more information.

# Licensees having purchased or holding a valid Flumotion Advanced
# Streaming Server license may use this file in accordance with the
# Flumotion Advanced Streaming Server Commercial License Agreement.
# See "LICENSE.Flumotion" in the source distribution for more information.

# Headers in this file shall remain intact.

from twisted.internet import glib2reactor
glib2reactor.install()
from twisted.internet import reactor
from twisted.internet import defer
# moving this down causes havoc when running this file directly for some reason

import gobject
gobject.threads_init()
import gst
import cluttergst
import clutter

import playlist

stage = clutter.Stage()
stage.set_size(720, 576)
stage.connect('destroy', lambda y: reactor.stop())
video_tex = clutter.Texture()
video_tex.set_size(720, 576)
size = stage.get_size()
print "%r" % (size,)
stage.add(video_tex)
props = { "video-pattern": "black", "audio-wave": "ticks", "width": 720, "height": 576, "base-directory": "/home/zaheer/playlist/files", "playlist-directory": "/home/zaheer/playlist/playlist", "texture": video_tex }

p = playlist.PlaylistProducer(props)
p.create_pipeline()
p.do_setup()
p.pipeline.set_state(gst.STATE_PLAYING)
overlay = clutter.Texture("overlay.png")
overlay.set_position(680, 10)
stage.add(overlay)
stage.show()

reactor.run()
