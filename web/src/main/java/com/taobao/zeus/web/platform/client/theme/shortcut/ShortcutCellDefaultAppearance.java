package com.taobao.zeus.web.platform.client.theme.shortcut;

import com.google.gwt.core.client.GWT;
import com.google.gwt.resources.client.ClientBundle;
import com.taobao.zeus.web.platform.client.widget.ShortcutCell.ShortcutCellAppearance;
import com.sencha.gxt.theme.base.client.button.ButtonCellDefaultAppearance;
import com.sencha.gxt.theme.base.client.frame.TableFrame;

public class ShortcutCellDefaultAppearance<C> extends ButtonCellDefaultAppearance<C> implements
    ShortcutCellAppearance<C> {

  public interface ShortcutCellResources extends ButtonCellResources, ClientBundle {
    @Source({"com/sencha/gxt/theme/base/client/button/ButtonCell.gss", "ShortcutCell.gss"})
    @Override
    ShortcutCellStyle style();
  }

  public interface ShortcutCellStyle extends ButtonCellStyle {
	  String select();
  }

  public ShortcutCellDefaultAppearance() {
    super(GWT.create(ShortcutCellResources.class),
        GWT.create(ButtonCellTemplates.class), new TableFrame(
            GWT.create(ShortcutTableFrameResources.class)));
  }

}
