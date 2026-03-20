# ============================================================
# DISPLAY MONITOR v1.2
# Trading Engine Monitor
# Production Safe Version
# ============================================================

import tkinter as tk
import json
import psutil
import threading

from signal_engine import SIGNALS, SIGNAL_LOCK


# ============================================================
# SINGLETON MONITOR
# ============================================================

class DisplayMonitor:

    _instance = None
    _lock = threading.Lock()


    # ========================================================
    # INIT
    # ========================================================

    def __init__(self, engine, trade_managers, nodes):

        with DisplayMonitor._lock:

            if DisplayMonitor._instance is not None:
                return

            DisplayMonitor._instance = self

        self.engine = engine
        self.trade_managers = trade_managers
        self.nodes = nodes

        self.refresh_rate = 500

        self.root = tk.Tk()
        self.root.title("Trading Engine Monitor")
        self.root.geometry("1300x850")
        self.root.configure(bg="black")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._build_layout()


    # ========================================================
    # SAFE CLOSE
    # ========================================================

    def _on_close(self):

        try:
            self.root.destroy()
        except Exception:
            pass

        DisplayMonitor._instance = None


    # ========================================================
    # BUILD LAYOUT
    # ========================================================

    def _build_layout(self):

        top_frame = tk.Frame(self.root, bg="black")
        top_frame.pack(fill="x", pady=5)

        self.engine_light = self._create_light(top_frame, "ENGINE")
        self.ws_light = self._create_light(top_frame, "WEBSOCKET")

        middle_frame = tk.Frame(self.root, bg="black")
        middle_frame.pack(fill="both", expand=True)

        self.manager_windows = []

        for tm in self.trade_managers:

            frame = tk.LabelFrame(
                middle_frame,
                text=f"{tm.signal_symbol}",
                fg="white",
                bg="black",
                padx=5,
                pady=5
            )

            frame.pack(side="left", fill="both", expand=True, padx=6, pady=4)

            text = tk.Text(
                frame,
                bg="black",
                fg="lime",
                insertbackground="lime"
            )

            text.pack(fill="both", expand=True)

            self.manager_windows.append((tm, text))

        banner_frame = tk.Frame(self.root, bg="black")
        banner_frame.pack(fill="x")

        self.signal_banner = tk.Label(
            banner_frame,
            text="GLOBAL SIGNAL BUS",
            fg="yellow",
            bg="black",
            font=("Courier", 12)
        )

        self.signal_banner.pack(fill="x")

        bottom = tk.Frame(self.root, bg="black")
        bottom.pack(fill="both", expand=True)

        left = tk.Frame(bottom, bg="black")
        left.pack(side="left", fill="both", expand=True)

        tk.Label(
            left,
            text="Parent Signals",
            fg="white",
            bg="black"
        ).pack()

        self.parent_signals = tk.Text(
            left,
            bg="black",
            fg="cyan"
        )

        self.parent_signals.pack(fill="both", expand=True)

        right = tk.Frame(bottom, bg="black")
        right.pack(side="right", fill="both", expand=True)

        tk.Label(
            right,
            text="Child Signals",
            fg="white",
            bg="black"
        ).pack()

        self.child_signals = tk.Text(
            right,
            bg="black",
            fg="orange"
        )

        self.child_signals.pack(fill="both", expand=True)


    # ========================================================
    # CREATE TRAFFIC LIGHT
    # ========================================================

    def _create_light(self, parent, label):

        frame = tk.Frame(parent, bg="black")
        frame.pack(side="left", padx=12)

        tk.Label(
            frame,
            text=label,
            fg="white",
            bg="black"
        ).pack()

        canvas = tk.Canvas(
            frame,
            width=20,
            height=20,
            bg="black",
            highlightthickness=0
        )

        canvas.pack()

        circle = canvas.create_oval(2, 2, 18, 18, fill="red")

        return (canvas, circle)


    # ========================================================
    # UPDATE STATUS LIGHTS
    # ========================================================

    def _update_lights(self):

        canvas, circle = self.engine_light
        canvas.itemconfig(circle, fill="green")

        canvas, circle = self.ws_light

        try:
            status = getattr(self.engine, "_is_ws_connected", False)
        except Exception:
            status = False

        color = "green" if status else "red"

        canvas.itemconfig(circle, fill=color)


    # ========================================================
    # UPDATE TRADE MANAGER WINDOWS
    # ========================================================

    def _update_managers(self):

        for tm, widget in self.manager_windows:

            widget.delete("1.0", tk.END)

            try:
                data = json.dumps(getattr(tm, "trade", {}), indent=4)
            except Exception:
                data = str(getattr(tm, "trade", {}))

            widget.insert(tk.END, data)


    # ========================================================
    # UPDATE SIGNAL BUS
    # ========================================================

    def _update_signals(self):

        try:

            with SIGNAL_LOCK:
                signals_copy = list(SIGNALS)

        except Exception:

            signals_copy = []

        parent = []
        child = []

        for s in reversed(signals_copy):

            symbol = s.get("symbol")

            if symbol and symbol.isalpha():
                parent.append(str(s))
            else:
                child.append(str(s))

        self.parent_signals.delete("1.0", tk.END)
        self.child_signals.delete("1.0", tk.END)

        for p in parent[:25]:
            self.parent_signals.insert(tk.END, p + "\n")

        for c in child[:25]:
            self.child_signals.insert(tk.END, c + "\n")


    # ========================================================
    # SYSTEM STATS
    # ========================================================

    def _update_system_stats(self):

        try:

            cpu = psutil.cpu_percent()
            mem = psutil.virtual_memory().percent

            self.root.title(
                f"Trading Engine Monitor — CPU {cpu}% | MEM {mem}%"
            )

        except Exception:
            pass


    # ========================================================
    # UPDATE LOOP
    # ========================================================

    def _update_loop(self):

        try:

            self._update_lights()
            self._update_managers()
            self._update_signals()
            self._update_system_stats()

        except Exception as e:

            print("[DISPLAY ERROR]", e)

        self.root.after(self.refresh_rate, self._update_loop)


    # ========================================================
    # START
    # ========================================================

    def start(self):

        if DisplayMonitor._instance is not self:
            return

        try:

            self._update_loop()
            self.root.mainloop()

        except Exception as e:

            print("[DISPLAY MONITOR ERROR]", e)

        finally:

            DisplayMonitor._instance = None